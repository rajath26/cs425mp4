#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<glib.h>
//#include"message_table.c"

#include <sys/types.h>
#include <sys/socket.h>
//#include <netinet/in.h>
//#include <arpa/inet.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/time.h>
#include <signal.h>

#define ERROR -1

struct op_code{
             int opcode;
             int key;
             char *value;
             char port[15];
             char IP[40];
             unsigned int timeStamp;
	     int owner;
             int friend1;
             int friend2;
             int cons_level;
};


struct value_group{
             char *value;
             int timestamp;
             int owner;
             int friend1;
             int friend2;
};


GQueue* temp_list=0x0;
GHashTable* key_value_store;

pthread_mutex_t key_value_mutex;

/*
 * THIS FUNCTION DELETES REPLICAS 
 * FROM FRIENDS
 */


int isOwnerAlive(gpointer value){
     funcEntry(logF,NULL,"isOwnerAlive");
     struct value_group* temp = (struct value_group *)value;
     int owneralive = 0;
     int i;
     for (i=0;i<MAX_HOSTS;i++){
             if(atoi(hb_table[i].host_id)==temp->owner){
                       if(hb_table[i].status==1 && hb_table[i].valid==1){
                                   owneralive = 1;
                       }
             }
     }
    if(owneralive){
       funcExit(logF,NULL,"isOwnerAlive",1);  
       return 1;
    }
    else
       funcExit(logF,NULL,"isOwnerAlive",0);
       return 0;
}



int areFriendsAlive(gpointer value){
     funcEntry(logF,NULL,"areFriendsAlive");
     struct value_group* temp = (struct value_group*)value;
     int friend1 = temp->friend1;
     int friend2 = temp->friend2;
     int friend1alive=0;
     int friend2alive=0;
     int i;
     // is first friend alive
     for (i=0;i<MAX_HOSTS;i++){
             if(atoi(hb_table[i].host_id)==friend1){
                       if(hb_table[i].status==1 && hb_table[i].valid==1){
                                   friend1alive = 1;
                       }
             }
     }

     // is second friend alive  
     for (i=0;i<MAX_HOSTS;i++){
             if(atoi(hb_table[i].host_id)==friend2){
                       if(hb_table[i].status==1 && hb_table[i].valid==1){
                                   friend2alive = 1;
                       }
             }
     }

     if(friend1alive && friend2alive){
           funcExit(logF,NULL,"areFriendsAlive",1);
           return 1;
     }
     else {
           funcExit(logF,NULL,"areFriendsAlive",0);
           return 0;
     }
}


int iAmOwner(gpointer value,int hash_value){
   funcEntry(logF,NULL,"iAmOwner");
   struct value_group* value_inst = (struct value_group *)value;
   sprintf(logMsg, "OWNER IN GPOINTER VALUE : %d;;;; PASSED IN HASH VALUE: %d", value_inst->owner, hash_value);
   printToLog(logF, "I AM OWNER DEBUG INFO", logMsg);
   
   if(value_inst->owner == hash_value) {
   funcExit(logF,NULL,"iAmOwner",1);
   return 1;
   }
   else{
   funcExit(logF,NULL,"iAmOwner",0);
   return 0;
   }
}



int delete_replica_from_friends(gpointer key, gpointer value, int chosenOwner)
{

    funcEntry(logF, NULL, "delete_replica_from_friends");
    
    int rc = 0;
    int i_rc;
    int friend1,
        index1,
        friend1Port,
        friend2,
        index2,
        friend2Port,
        friend1Socket,
        friend2Socket;
    char friend1IP[25];
    char friend2IP[25]; 
    char deleteMsg[4096];
    char deleteResponse[4096];
    struct sockaddr_in friend1Addr;
    int numOfBytesSent;
    int hisFriendList[2];
    int hisFren1, hisFren2;

    struct op_code temp1,
                   temp2;

    if ( chosenOwner == host_no )
    { 

        i_rc = chooseFriendsForReplication(hisFriendList);
        if ( i_rc != 0 )
        {
            hisFren1 = 0;
            hisFren2 = 0;
        }
        else
        {
           hisFren1 = giveIndexForHash(hisFriendList[0]);
           hisFren2 = giveIndexForHash(hisFriendList[1]);
        } 

    }
    else
    {

        i_rc = chooseFriendsForHim(hisFriendList, atoi(hb_table[chosenOwner].host_id));
        if ( i_rc != 0 )
        {
            hisFren1 = 0;
            hisFren2 = 0;
        }
        else
        {
           hisFren1 = giveIndexForHash(hisFriendList[0]);
           hisFren2 = giveIndexForHash(hisFriendList[1]);
        }

    }

    // Get friend1 hash value in the chord
    friend1 = ((struct value_group *)value)->friend1;
    
    // Get the index of friend 1 from the membership protocol
    index1 = giveIndexForHash(friend1);
    if ( ERROR == index1 )
    {
        // If index is not returned properly then we will not be able 
        // to delete replicas which is still not a hard stop 
        printToLog(logF, ipAddress, "Friend1 index cannot be retrieved");
        rc = -1;
        goto friend2Label;
    }

    if ( index1 == chosenOwner || index1 == hisFren1 || index1 == hisFren2 )
        goto friend2Label; 

    friend1Port = atoi(hb_table[index1].port);
    strcpy(friend1IP, hb_table[index1].IP);

    sprintf(logMsg, "Friend1 from whom replica will be deleted; friend1 hash in chord: %d; friend1 port: %d; friend1 IP: %s", friend1, friend1Port, friend1IP);
    printToLog(logF, ipAddress, logMsg);

    friend1Socket = socket(AF_INET, SOCK_STREAM, 0);
    if ( ERROR == friend1Socket )
    {
        sprintf(logMsg, "Unable to open socket. Error number: %d", errno);
        printToLog(logF, ipAddress, logMsg);
        rc = -1;
        goto friend2Label;
    }

    memset(&friend1Addr, 0, sizeof(struct sockaddr_in));
    friend1Addr.sin_family = AF_INET;
    friend1Addr.sin_port = htons(friend1Port);
    friend1Addr.sin_addr.s_addr = inet_addr(friend1IP);
    memset(&(friend1Addr.sin_zero), '\0', 8);

    i_rc = connect(friend1Socket, (struct sockaddr *) &friend1Addr, sizeof(friend1Addr));
    if ( SUCCESS != i_rc )
    {
        strcpy(logMsg, "Cannot connect to server during deletion of replicas in leaveSystem");
        printToLog(logF, ipAddress, logMsg);
        printf("\n%s\n", logMsg); 
        rc = -1;
        goto friend2Label;
    }

    temp1.key = atoi((char *)key);
    temp1.value = NULL;
    temp1.owner = my_hash_value;
    temp1.friend1 = ((struct value_group *)value)->friend1;
    temp1.friend2 = ((struct value_group *)value)->friend2;
               
    create_message_REP_DELETE(&temp1, deleteMsg);
    append_port_ip_to_message(hb_table[host_no].port,hb_table[host_no].IP,deleteMsg);
    append_time_consistency_level(-1, 0, deleteMsg);
 
    numOfBytesSent = sendTCP(friend1Socket, deleteMsg, sizeof(deleteMsg));
    if ( 0 == numOfBytesSent || -1 == numOfBytesSent )
    {
        printToLog(logF, ipAddress, "ZERO BYTES SENT TO FRIEND1 WHILE DELETING REPLICA DURING LEAVE");
        rc = -1;
        goto friend2Label;
        // This is not a hard stop so continue
    }

    int numOfBytesRec = recvTCP(friend1Socket, deleteResponse, sizeof(deleteResponse));
    if ( 0 == numOfBytesRec || -1 == numOfBytesRec )
    {
        printToLog(logF, ipAddress, "ZERO BYTES RECEIVED FROM FRIEND1 WHILE DELETING REPLICA DURING LEAVE");
        rc = -1;
        goto friend2Label;
        // This is not a hard stop so continue
    }

    friend2Label:

               memset(deleteMsg, '\0', 4096);
               memset(deleteResponse, '\0', 4096);
               // Get friend2 hash value in the chord
               friend2 = ((struct value_group *)value)->friend2;

               // Get the index of friend 1 from the membership protocol
               index2 = giveIndexForHash(friend2);
               if ( ERROR == index2 )
               {
                   // If index is not returned properly then we will not be able 
                   // to delete replicas which is still not a hard stop 
                   printToLog(logF, ipAddress, "Friend2 index cannot be retrieved");
                   rc = -1;
                   goto rtn;
               }

               if ( index2 == chosenOwner || index2 == hisFren1 || index2 == hisFren2 )
                   goto rtn;

               friend2Port = atoi(hb_table[index2].port);
               strcpy(friend2IP, hb_table[index2].IP);

               sprintf(logMsg, "Friend2 from whom replica will be deleted; friend2 hash in chord: %d; friend2 port: %d; friend2 IP: %s", friend2, friend2Port, friend2IP);
               printToLog(logF, ipAddress, logMsg);

               friend2Socket = socket(AF_INET, SOCK_STREAM, 0);
               if ( ERROR == friend2Socket )
               {
                   sprintf(logMsg, "Unable to open socket. Error number: %d", errno);
                   printToLog(logF, ipAddress, logMsg);
                   rc = -1;
                   goto rtn;
               }
               struct sockaddr_in friend2Addr;
               memset(&friend2Addr, 0, sizeof(struct sockaddr_in));
               friend2Addr.sin_family = AF_INET;
               friend2Addr.sin_port = htons(friend2Port);
               friend2Addr.sin_addr.s_addr = inet_addr(friend2IP);
               memset(&(friend2Addr.sin_zero), '\0', 8);

               i_rc = connect(friend2Socket, (struct sockaddr *) &friend2Addr, sizeof(friend2Addr));
               if ( SUCCESS != i_rc )
               {
                   strcpy(logMsg, "Cannot connect to server during deletion of replicas in leaveSystem");
                   printToLog(logF, ipAddress, logMsg);
                   printf("\n%s\n", logMsg); 
                   rc = -1;
                   goto rtn;
               }

               temp2.key = atoi((char *)key);
               temp2.value = NULL;
               temp2.owner = my_hash_value;
               temp2.friend1 = ((struct value_group *)value)->friend1;
               temp2.friend2 = ((struct value_group *)value)->friend2;

               create_message_REP_DELETE(&temp2, deleteMsg);
               append_port_ip_to_message(hb_table[host_no].port,hb_table[host_no].IP,deleteMsg);
               append_time_consistency_level(-1, 0, deleteMsg);

               numOfBytesSent = sendTCP(friend2Socket, deleteMsg, sizeof(deleteMsg));
               if ( 0 == numOfBytesSent || -1 == numOfBytesSent )
               {
                   printToLog(logF, ipAddress, "ZERO BYTES SENT TO FRIEND2 WHILE DELETING REPLICA DURING LEAVE");
                   rc = -1;
                   goto rtn;
                   // This is not a hard stop so continue
               }

               numOfBytesRec = recvTCP(friend2Socket, deleteResponse, sizeof(deleteResponse));
               if ( 0 == numOfBytesRec || -1 == numOfBytesRec )
               {
                   printToLog(logF, ipAddress, "ZERO BYTES RECEIVED FROM FRIEND2 WHILE DELETING REPLICA DURING LEAVE");
                   rc = -1;
                   goto rtn;
                   // This is not a hard stop so continue
               }

  rtn:
    if ( -1 != friend1Socket )
             close(friend1Socket);
    if ( -1 != friend2Socket )
             close(friend2Socket);

    funcExit(logF, NULL, "delete_replica_from_friends", rc);    
    return rc;
    
}

int prepare_system_for_leave(gpointer key,gpointer value, gpointer dummy)
{

         funcEntry(logF,NULL,"prepare_system_for_leave");

         int rc = 1;
         
         char port[20];
         char IP[100];
         char response[4096];
         int sd;
         int i_rc;
         struct sockaddr_in peer;
         guint m = g_hash_table_size(key_value_store);
         char message[4096];

         if(m!=0)
         {
 
               update_host_list();

               // Check if this host is the owner of this KV pair
               if (!iAmOwner(value, my_hash_value))
               {
                   printToLog(logF, ipAddress, "This is just a replica copy of another guy so ignore");
                   rc = 1;
                   goto rtn;
               }

               // For all the entries that belong to this host:
               // 1) rehash 
               // 2) send it to the peer node 
               // 3) delete the replica of this entry

               // 1) Re-hash the key
               int i = choose_host_hb_index(atoi((char*)key));

               // 2) Send the insert message to the peer node 
               memset(message, '\0', 4096);
               create_message_INSERT_LEAVE(atoi((char *)key),((struct value_group *)value)->value,message);
               append_port_ip_to_message(hb_table[host_no].port,hb_table[host_no].IP,message);
               append_time_consistency_level(-1, 0, message);
               strcpy(port,hb_table[i].port);
               strcpy(IP,hb_table[i].IP);
               sprintf(logMsg, "PEER NODE CHOSEN. IP ADDRESS: %s PORT NO: %s", port, IP);
               printf("\n%s\n", logMsg);
               printToLog(logF, "PEER NODE CHOSEN", logMsg);

               sd = socket(AF_INET, SOCK_STREAM, 0);
               if ( -1 == sd )
               {
                    sprintf(logMsg, "\nUnable to open socket in prepare_system_for_leave. ERROR NO: %d\n", errno);
                    printToLog(logF, "SOCKET ERROR", logMsg);
                    printf("\n%s\n", logMsg);
                    perror("socket");
                    rc = 0;
                    goto rtn;
               }

               memset(&peer, 0, sizeof(struct sockaddr_in));
               peer.sin_family = AF_INET;
               peer.sin_port = htons(atoi(port));
               peer.sin_addr.s_addr = inet_addr(IP);
               memset(&(peer.sin_zero), '\0', 8);

               i_rc = connect(sd, (struct sockaddr *) &peer, sizeof(peer));
               if ( i_rc != 0 )
               {
                   sprintf(logMsg, "\nCant connect to server in prepare_system_for_leave\n. ERROR NO: %d", errno);
                   printf("\n%s\n", logMsg);
                   printToLog(logF, "CONNECT ERROR", logMsg);
                   perror("connect");
                   rc = 0;
                   goto rtn;
               }

               printToLog(logF, ipAddress, "Sending INSERT MESSAGE TO PEER NODE BEFORE LEAVING");
               int numOfBytesSent = sendTCP(sd, message, sizeof(message));
               if ( 0 == numOfBytesSent || -1 == numOfBytesSent )
               {
                   printf("\nZERO BYTES SENT IN prepare_system_for_leave\n");
                   rc = 0;
                   goto rtn;
               }

               int numOfBytesRec = recvTCP(sd, response, 4096);
               if ( 0 == numOfBytesRec || -1 == numOfBytesRec )
               {
                   printf("\nZERO BYTES RECEIVED IN prepare_system_for_leave\n");
                   rc = 0;
                   goto rtn;
               }

               //delete_key_value_from_store(atoi((char *)key));

               // 3) DELETE THE REPLICAS of this entry
               i_rc = delete_replica_from_friends(key, value, i);
               if ( -1 == i_rc )
               {
                   printToLog(logF, ipAddress, "Error during deletion of replicas from friends");
                   printToLog(logF, ipAddress, "Not a hard stop continue");
               }

         }
         else  
         {
             // Table is empty return 0
             rc = 0;
             goto rtn;
         }
      rtn:
         if ( -1 != sd )
             close(sd);
         
         funcExit(logF,NULL,"prepare_system_for_leave",rc);
         return rc;
}

void prepareNodeForSystemLeave(){
         funcEntry(logF,NULL,"prepareNodeForSystemLeave");
         pthread_mutex_lock(&table_mutex);
         hb_table[host_no].valid = 0;
         hb_table[host_no].status = 0;
         pthread_mutex_unlock(&table_mutex);
         update_host_list();
         guint m = g_hash_table_size(key_value_store);
         sprintf(logMsg, "The Hash table size during leaveSystem is : %d", (int)m);
         printToLog(logF, "THE HASH TABLE SIZE DURING LEAVESYSTEM IS", logMsg);
         printf("\n%s\n", logMsg);
          if(m==0)
          {
              return;
          }
         //pthread_mutex_lock(&key_value_mutex);
         g_hash_table_foreach_remove(key_value_store,prepare_system_for_leave,NULL);
         //pthread_mutex_unlock(&key_value_mutex);
         funcExit(logF,NULL,"prepareNodeForSystemLeave",0);
}

int create_temp_list()
{
   funcEntry(logF,NULL,"create_temp_list");
   temp_list =  g_queue_new();
   funcExit(logF,NULL,"choose_temp_list",0);
}


int insert_into_temp_list(int key, char *value)
{
   funcEntry(logF,NULL,"insert_into_temp_list");
   struct op_code* temp = (struct op_code *)malloc(sizeof(struct op_code));  
   temp->key = key;
   temp->value = value;
   g_queue_push_tail(temp_list,(gpointer)temp);
   funcExit(logF,NULL,"insert_into_temp_list",0);     
}

struct op_code* retrieve_from_temp_list()
{
   funcEntry(logF,NULL,"retrieve_from_temp_list");
   struct op_code* temp = g_queue_pop_head(temp_list);
   funcExit(logF,NULL,"retrieve_from_temp_list",0);
   return temp;
}

void print_key_value(gpointer key,gpointer value, gpointer dummy){
         funcEntry(logF,NULL,"print_key_value");
         int yesIamOwner = 0;
         //pthread_mutex_lock(&key_value_mutex);
         if (iAmOwner(value, my_hash_value))
             yesIamOwner = 1;
         printf("\n\t*******************************\n");
         printf("\tKEY : %s - VALUE : %s ::: I am owner ?: %d ::: Owner: %d ::: F1 : %d ::: F2 : %d \n",(char *)key,((struct value_group*)value)->value, yesIamOwner, ((struct value_group*)value)->owner, ((struct value_group*)value)->friend1, ((struct value_group*)value)->friend2);
         printf("\n\t*******************************\n");
          
         //pthread_mutex_unlock(&key_value_mutex);
         funcExit(logF,NULL,"print_key_value",0);
}

int process_key_value(gpointer key,gpointer value, gpointer dummy)
{

   
    funcEntry(logF,NULL,"process_key_value");
    int rc = 0;
    update_host_list();
    int i = choose_host_hb_index(atoi((char*)key));         
    int i_rc;
    int numOfBytesSent;
    int sd;
    char port[20];
    char IP[100];
    char message[4096];
    char recMsg[4096];
    char response[4096];
    struct sockaddr_in peer;
    int friendsAlive = 1;
    int friendList[2];
    struct op_code* temp = (struct op_code *) malloc(sizeof(struct op_code));
    int tempAck;

    sprintf(logMsg, "choose hb_index ret i %d; Port : %s; IP : %s", i, hb_table[i].port, hb_table[i].IP);
    printToLog(logF, "p_k_v choose_hb_index", logMsg);
    strcpy(port,hb_table[i].port);
    strcpy(IP,hb_table[i].IP);
          
    // Get the hash table size 
    // if it is empty return
    guint m = g_hash_table_size(key_value_store);
    if(m==0)
    {
        rc = 0;
        return;
    }
 
    /////
    // 1) Check if you are the owner of this entry
    /////
    if (iAmOwner(value, my_hash_value))
    {

        printToLog(logF, "p_k_v", "I am owner of this entry");
        /////
        // 2) If you are the owner of the entry and it has rehashed to another 
        // peer node then
        /////
        if (i != host_no)
        {

            int fHim[2];

            printToLog(logF, "p_k_v", "Rehashed to someone else so sending the KV to them and attempt delete replicas");

            // 2i) Send it to rehashed peer node
            memset(message, '\0', 4096);
            create_message_INSERT(atoi((char *)key),((struct value_group *)value)->value,message);
            sprintf(logMsg, "PORT: %s, IP : %s , message: %s", hb_table[i].port, hb_table[i].IP, message);
            printToLog(logF, "PROCESS_KEY_VALUE", logMsg);
            append_port_ip_to_message(hb_table[host_no].port,hb_table[host_no].IP,message);         
            append_time_consistency_level(-1, 0, message);
            
            sd = socket(AF_INET, SOCK_STREAM, 0);
            if ( -1 == sd )
            {
                printf("\nUnable to open socket in prepare_system_for_leave\n");
                goto rtn;
            }

            printToLog(logF, "p_k_v", "Socket successful");
            
            memset(&peer, 0, sizeof(struct sockaddr_in));
            peer.sin_family = AF_INET;
            peer.sin_port = htons(atoi(hb_table[i].port));
            peer.sin_addr.s_addr = inet_addr(hb_table[i].IP);
            memset(&(peer.sin_zero), '\0', 8);

            i_rc = connect(sd, (struct sockaddr *) &peer, sizeof(peer));
            if ( i_rc != 0 )
            {
               printf("\nCant connect to server in prepare_system_for_leave\n");
               goto rtn;
            }

            printToLog(logF, "p_k_v", "Connect successful");

            int numOfBytesSent = sendTCP(sd, message, sizeof(message));
            if ( 0 == numOfBytesSent || -1 == numOfBytesSent )
            {
               printf("\nZERO BYTES SENT IN prepare_system_for_leave\n");
               goto rtn;
            }

            printToLog(logF, "p_k_v", "Send successful");

            int numOfBytesRec = recvTCP(sd, response, 4096);
            if ( 0 == numOfBytesRec || -1 == numOfBytesRec )
            {
               printf("\nZERO BYTES RECEIVED IN prepare_system_for_leave\n");
               goto rtn;
            }

            printToLog(logF, "p_k_v", "Receive Successful in HEREE HEREHEREE ");

            chooseFriendsForHim(fHim, atoi(hb_table[i].host_id));
            
            // 2ii) Delete from the KV store
            //delete_key_value_from_store(atoi((char *)key));
            if ( my_hash_value != fHim[0] && my_hash_value != fHim[1])
                rc = 1;
            else 
                rc = 0;

            // 2iii) Delete the replicas
            i_rc = delete_replica_from_friends(key, value, i);
            if ( -1 == i_rc )
            {
                printToLog(logF, ipAddress, "Deletion of replicas on friends failed. But this is not a hard stop");
            }
         
        } // End of if (i != host_no)
        /////
        // 3) If you are the owner of the entry and it rehashed to yourself
        /////
        else
        {

            printToLog(logF, "p_k_v", "Rehashed to myself so find my friends as of now. See if the found friends as of now are same as hash table and hence determine any change and act accordingly");

            int friendsNow[2];
            int missingIndex;
            int bothFriendsDead = 0;
            int friend1Missing = 0;
            int friend2Missing = 0;
            int friendsNowFirstChosen = 0;
            int friendsNowSecondChosen = 0;
   
            // Check who are my friends as of now 
            chooseFriendsForReplication(friendsNow);

            // Check if the friends chosen as of now are same as the current value
            if ( ( ((friendsNow[0] == ((struct value_group *)value)->friend1) || (friendsNow[0] == ((struct value_group *)value)->friend2)) ) && ( ( (friendsNow[1] == ((struct value_group *)value)->friend1) || (friendsNow[1] == ((struct value_group *)value)->friend2) ) ) )
            {
                printToLog(logF, "p_k_v", "IGNORE");
                // No topology change OR
                // Friends Alive 
                // so IGNORE
            }
            else
            {
                printToLog(logF, "p_k_v", "OOPS SOMETHING HAS CHANGED");
                if ( ((friendsNow[0] != ((struct value_group *)value)->friend1) && (friendsNow[0] != ((struct value_group *)value)->friend2)) && ((friendsNow[1] == ((struct value_group *)value)->friend1) || (friendsNow[1] == ((struct value_group *)value)->friend2) ) )
                {
                    missingIndex = giveIndexForHash(friendsNow[0]);
                    friendsNowFirstChosen = 1;
                    if ( friendsNow[1] == ((struct value_group *)value)->friend1 )
                       friend2Missing = 1;
                    else
                       friend1Missing = 1; 
                }
                else if ( ((friendsNow[1] != ((struct value_group *)value)->friend1) && (friendsNow[1] != ((struct value_group *)value)->friend2)) && ((friendsNow[0] == ((struct value_group *)value)->friend1) || (friendsNow[0] == ((struct value_group *)value)->friend2) ) )
                {
                    missingIndex = giveIndexForHash(friendsNow[1]);
                    friendsNowSecondChosen = 1;
                    if ( friendsNow[0] == ((struct value_group *)value)->friend1 )
                       friend2Missing = 1;
                    else
                       friend1Missing = 1;
                }
                else if ( ((friendsNow[0] != ((struct value_group *)value)->friend1) && (friendsNow[0] != ((struct value_group *)value)->friend2)) && ((friendsNow[1] != ((struct value_group *)value)->friend1) && (friendsNow[1] != ((struct value_group *)value)->friend2)) )
                    bothFriendsDead = 1;

                if (!bothFriendsDead)
                {
                    printToLog(logF, "p_k_v", "only one friend dead");
                    sprintf(logMsg, "Dead friend replaced by this guy : %d", atoi(hb_table[missingIndex].host_id));
                    printToLog(logF, "p_k_v", logMsg); 

                    int missingPort = atoi(hb_table[missingIndex].port);
                    char missingIP[100];
                    char repMsg[4096];
                    char response[4096];
                    struct sockaddr_in missingAddr;
                    struct op_code missingOpCode;
                    strcpy(missingIP, hb_table[missingIndex].IP);

                    int missingSocket = socket(AF_INET, SOCK_STREAM, 0);

                    memset(&missingAddr, 0, sizeof(struct sockaddr_in));
                    missingAddr.sin_family = AF_INET;
                    missingAddr.sin_port = htons(missingPort);
                    missingAddr.sin_addr.s_addr = inet_addr(missingIP);
                    memset(&(missingAddr.sin_zero), '\0', 8);
 
                    connect(missingSocket, (struct sockaddr *) &missingAddr, sizeof(missingAddr));

                    missingOpCode.key = atoi((char *)key);
                    missingOpCode.value = ((struct value_group *)value)->value;
                    missingOpCode.owner = my_hash_value;
                    if (friend1Missing)
                    {
                        missingOpCode.friend1 = atoi(hb_table[missingIndex].host_id);
                        if (friendsNowFirstChosen)
                            missingOpCode.friend2 = friendsNow[1];
                        else
                            missingOpCode.friend2 = friendsNow[0];
                    }
                    if (friend2Missing)
                    {
                        missingOpCode.friend2 = atoi(hb_table[missingIndex].host_id);
                        if (friendsNowFirstChosen)
                            missingOpCode.friend1 = friendsNow[1];
                        else
                            missingOpCode.friend1 = friendsNow[0];
                    }

                    create_message_REP_INSERT(&missingOpCode, repMsg);
                    append_port_ip_to_message(hb_table[host_no].port,hb_table[host_no].IP,repMsg);
                    append_time_consistency_level(-1, 0, repMsg);

                    sendTCP(missingSocket, repMsg, 4096);
 
                    recvTCP(missingSocket, response, 4096);

                    close(missingSocket);

                    printToLog(logF, "p_k_v", "Updating local KV store");
                    update_key_value_in_store(&missingOpCode);
                    
                } // End of if (!bothFriendsDead)
                else
                {

                    printToLog(logF, "p_k_v", "Both friends dead");

                    struct op_code temp1;
                    //char inMsg[4096];
                    int myF[2];

                    //create_message_INSERT(atoi((char *)key), ((struct value_group *)value)->value, inMsg);
                    //append_port_ip_to_message(hb_table[host_no].port, hb_table[host_no].IP, inMsg);
                    //append_time_consistency_level(-1, 0, inMsg);

                    //int i_temp = extract_message_op(inMsg, &temp);
                    //if ( -1 == i_temp )
                        //goto rtn;
                    
                    temp1.opcode = 1;
                    temp1.key = atoi((char *)key);
                    temp1.value = ((struct value_group *)value)->value;
                    strcpy(temp1.port, hb_table[host_no].port);
                    strcpy(temp1.IP, hb_table[host_no].IP);
                    temp1.timeStamp = 0;
                    temp1.cons_level = 0;

                    chooseFriendsForReplication(myF);
                    
                    temp1.owner = my_hash_value;
                    temp1.friend1 = myF[0];
                    temp1.friend2 = myF[1];

                    // TO DO load time stamp
                
                    // 3ii) If your friends are not alive then re-replicate
                    tempAck = replicateKV(&temp1, myF);
                    if (2 != tempAck)
                    {
                        printToLog(logF, ipAddress, "One or both of my friends had died and I tried to replicate and that also failed :(");
                        printToLog(logF, ipAddress, "Not a hard stop continue");
                    }       
 
                    printToLog(logF, "p_k_v", "Updating my current entry");
                    update_key_value_in_store(&temp1);

                } // End of else of if (!bothFriendsDead)
            } // End of else
        } // End of else of if (i != host_no)

    } // End of if (iAmOwner(value, my_hash_value))
    /////
    // 4) If you are not the owner of the entry
    /////
    else
    {

        printToLog(logF, "p_k_v", "I am not owner");

        if(isOwnerAlive(value))
        {
            //int hisFriends[2];
            printToLog(logF, ipAddress, "Owner of this entry alive. SO check his friends as of now and update local entry");
            chooseFriendsForHim(friendList, ((struct value_group*)value)->owner);
            // Check if you are in the new friends list of him
            if ( my_hash_value == friendList[0] || my_hash_value == friendList[1] )
            {
                printToLog(logF, "p_k_v", "I am in his new friends list so update");
                struct op_code update;
                update.key = atoi((char *)key);
                update.value = ((struct value_group *)value)->value;
                update.owner = ((struct value_group *)value)->owner;
                update.friend1 = friendList[0];
                update.friend2 = friendList[1];
 
                update_key_value_in_store(&update);
            }
            else
            {
                printToLog(logF, "p_k_v", "I am not in hisFriends list anymore hence delete this entry");
                rc = 1;
            }
        }
        else
        {
 
            printToLog(logF, "p_k_v", "Owner dead. Check if you are closest friend or closest friend is dead. If either is true find entry new owner");
            int closestFriend = 0;
            int closestFriendDead = 0;
            if (((struct value_group *)value)->friend1 == my_hash_value)
                closestFriend = 1;
            else 
            {
                // Find if Closest friend is alive
                int closestFrenIndex = giveIndexForHash(((struct value_group *)value)->friend1);
                if ( hb_table[closestFrenIndex].status == 0 && hb_table[closestFrenIndex].valid == 0 )
                    closestFriendDead = 1;
            }
            if (closestFriend)
                printToLog(logF, "p_k_v", "I am closest friend");
            else 
                printToLog(logF, "p_k_v", "Closest friend dead");
            if ( closestFriend || closestFriendDead )
            {
                memset(message, '\0', 4096);
                create_message_INSERT(atoi((char *)key),((struct value_group *)value)->value,message);
                sprintf(logMsg, "PORT: %s, IP : %s , message: %s", hb_table[i].port, hb_table[i].IP, message);
                printToLog(logF, "PROCESS_KEY_VALUE", logMsg);
                append_port_ip_to_message(hb_table[host_no].port,hb_table[host_no].IP,message);         
                append_time_consistency_level(-1, 0, message);

                /*if ( (0 == strcmp(port, hb_table[host_no].port)) && (0 == strcmp(IP, hb_table[host_no].IP)) && i == host_no )
                {

                    printToLog(logF, "p_k_v", "OWNER DEAD AND I AM ONLY NEW OWNER SO LOCAL INSERT");

                    // Owner is dead and it rehashed to me itself so just do a local insert
                    struct op_code local;
                    int myfriends[2];
                    i_rc = chooseFriendsForReplication(myfriends);
                    if ( -1 == i_rc )
                        goto socket;

                    local.key = atoi((char *)key);
                    local.value = ((struct value_group *)value)->value;
                    local.owner = my_hash_value;
                    local.friend1 = myfriends[0];
                    local.friend2 = myfriends[1];
    
                    insert_key_value_into_store(&local);
 
                    rc = 0;

                    goto delete_replica;

                }*/
            
                printToLog(logF, "p_k_v", "Owner dead and sending INSERT to new owner");

                sd = socket(AF_INET, SOCK_STREAM, 0);
                if ( -1 == sd )
                {
                    printf("\nUnable to open socket in prepare_system_for_leave\n");
                    goto rtn;
                }
                
                memset(&peer, 0, sizeof(struct sockaddr_in));
                peer.sin_family = AF_INET;
                peer.sin_port = htons(atoi(hb_table[i].port));
                peer.sin_addr.s_addr = inet_addr(hb_table[i].IP);
                memset(&(peer.sin_zero), '\0', 8);

                i_rc = connect(sd, (struct sockaddr *) &peer, sizeof(peer));
                if ( i_rc != 0 )
                {
                   printf("\nCant connect to server in prepare_system_for_leave\n");
                   goto rtn;
                }

                int numOfBytesSent = sendTCP(sd, message, sizeof(message));
                if ( 0 == numOfBytesSent || -1 == numOfBytesSent )
                {
                    printf("\nZERO BYTES SENT IN prepare_system_for_leave\n");
                    goto rtn;
                }

                int numOfBytesRec = recvTCP(sd, response, 4096);
                if ( 0 == numOfBytesRec || -1 == numOfBytesRec )
                {
                   printf("\nZERO BYTES RECEIVED IN prepare_system_for_leave\n");
                   goto rtn;
                }

                // 2ii) Delete from the KV store
                //delete_key_value_from_store(atoi((char *)key));
                // If I am only owner then do not delete
                if (! ((0 == strcmp(hb_table[i].port, hb_table[host_no].port)) && (0 == strcmp(hb_table[i].IP, hb_table[host_no].IP)) && i == host_no )) 
                    rc = 1;

                // 2iii) Delete the replicas
                i_rc = delete_replica_from_friends(key, value, i);
                if ( -1 == i_rc )
                {
                    printToLog(logF, ipAddress, "Deletion of replicas on friends failed. But this is not a hard stop");

                } 

            } // End of if ( closestFriend || closestFriendDead )

        } // End of else of if(isOwnerAlive)
    } // End of else of if (iAmOwner(value, my_hash_value))

    rtn:
        if ( -1 != sd )
            close(sd);
        funcExit(logF,NULL,"process_key_value",0);
        return rc;

}
void reorganize_key_value_store(){
         funcEntry(logF,NULL,"reorganize_key_value_store");
         if (systemIsLeaving)
             return;
         guint m = g_hash_table_size(key_value_store);
         if(m==0) return;
         //pthread_mutex_lock(&key_value_mutex);
         g_hash_table_foreach_remove(key_value_store,process_key_value,NULL);
         //pthread_mutex_unlock(&key_value_mutex);
         reOrderTrigger=0;         
         funcExit(logF,NULL,"reorganize_key_value_store",0);
}

void iterate_hash_table(){
         funcEntry(logF,NULL,"iterate_hash_table");
//         pthread_mutex_lock(&key_value_mutex);
         guint size = g_hash_table_size(key_value_store);
         printf("\nHASH TABLE SIZE: %d\n", (int)size);
         //g_hash_table_foreach(key_value_store,print_key_value,NULL);
//         pthread_mutex_unlock(&key_value_mutex);
         funcExit(logF,NULL,"iterate_hash_table",0);
}
/*
void print_key_value(char *key,char *value){
         printf("key :%s, value : %s\n",key,value);
}*/

int create_hash_table(){
   funcEntry(logF,NULL,"create_hash_table");
   //pthread_mutex_lock(&key_value_mutex);
   key_value_store =  g_hash_table_new(g_str_hash,g_str_equal);
   if(key_value_store == NULL){
             //pthread_mutex_unlock(&key_value_mutex); 
             funcExit(logF,NULL,"create_hash_table",0);
             return -1;
   }
   else{
             //pthread_mutex_unlock(&key_value_mutex); 
             funcExit(logF,NULL,"create_hash_table",0);
             return 0;
   }
}


char* lookup_store_for_key(int key);
// send an opcode instance which is dynamically allocated
//
int insert_key_value_into_store(struct op_code* op_instance){
   
     funcEntry(logF,NULL,"insert_key_value_into_store");  
     pthread_mutex_lock(&key_value_mutex);
     char *buffer;
     buffer = (char*)malloc(200);
     sprintf(buffer,"%d",op_instance->key);

     /*char* value_old = lookup_store_for_key(op_instance->key);
     if(value_old){
          delete_key_value_from_store(op_instance->key);
      }*/

     gpointer key = (gpointer)buffer;

/*
struct value_group{
             char *value;
             int timestamp;
             int owner;
             int friend1;
             int friend2;
};
*/
    struct value_group* value_obj = (struct value_group *)malloc(sizeof(struct value_group));
    value_obj->value = op_instance->value;
    value_obj->timestamp = op_instance->timeStamp;
    value_obj->owner = op_instance->owner;
    value_obj->friend1 = op_instance->friend1;
    value_obj->friend2 = op_instance->friend2;


     gpointer value = (gpointer)value_obj;

     g_hash_table_replace(key_value_store,key,value);
     pthread_mutex_unlock(&key_value_mutex);
     funcExit(logF,NULL,"insert_key_value_into_store",0);
}

char* lookup_store_for_key(int key){
    
     funcEntry(logF,NULL,"lookup_store_for_key");
     pthread_mutex_lock(&key_value_mutex);
     gpointer value;
     char *buffer = (char *)malloc(200);
     sprintf(buffer,"%d",key);
     gpointer key_temp = (gpointer)buffer;
     value = g_hash_table_lookup(key_value_store,key_temp);

     if(value == NULL)
       return NULL;

     free(buffer);
     pthread_mutex_unlock(&key_value_mutex);
     funcExit(logF,NULL,"lookup_store_for_key",0);
     return ((struct value_group *)value)->value;
}

int update_key_value_in_store(struct op_code *op_instance)
{
    funcEntry(logF, NULL, "update_key_value_into_store");
    int rc = 0,i_rc;
    if(op_instance == NULL)
        return -1;
    char* lookupValue = lookup_store_for_key(op_instance->key);
    if ( NULL == lookupValue) {
        rc = -1;
        goto rtn;
    }
    else{
        i_rc = insert_key_value_into_store(op_instance);
        if ( -1 == i_rc ){
            rc = -1;
            goto rtn;
        } 
    }
 rtn:
    funcExit(logF, NULL, "update_key_value_into_store", rc);
    return rc;
} // End of update_key_value_in_store


int delete_key_value_from_store(int key){
     funcEntry(logF,NULL,"delete_key_value_from_store");
     pthread_mutex_lock(&key_value_mutex);
     int status;
     char *buffer = (char *)malloc(200);
     sprintf(buffer,"%d",key);
   
     gpointer value;  // free_code
     value = g_hash_table_lookup(key_value_store,buffer); // free_code

     status = g_hash_table_remove(key_value_store,buffer);
     if(status){
          
          free((struct value_group *)value);   // free_code
          free(buffer); // free_code

          funcExit(logF,NULL,"delete_key_value_from_store",0);
          pthread_mutex_unlock(&key_value_mutex); 
          return 0; //success
     }
     else{

          free(buffer);  // free_code

          funcExit(logF,NULL,"delete_key_value_from_store",0);
          pthread_mutex_unlock(&key_value_mutex);
          return -1; //failure
    }
}
int append_time_consistency_level(unsigned int timestamp, int consistency_level, char *message){
                   funcEntry(logF,NULL,"append_time_consistency_level");
                   char buf[20];

                   if(timestamp==-1){
    			 struct timeval timer;
                         gettimeofday(&timer,NULL);                    
                         timestamp = timer.tv_sec;
                   }                   

                   sprintf(buf,"%d",timestamp);
                   strcat(message,buf);
                   strcat(message,":");
                   char buf1[20];
                   sprintf(buf1,"%d",consistency_level);
                   strcat(message,buf1);
                   strcat(message,";");
                   funcExit(logF,NULL,"append_time_consistency_level",0);
                   return 0;
}
                   

int append_port_ip_to_message(char *port,char *ip,char *message){
                   funcEntry(logF,NULL,"append_port_ip_to_message");
                   strcat(message,port);
                   strcat(message,":");
                   strcat(message,ip);
                   strcat(message,";");
                   funcExit(logF,NULL,"append_port_ip_to_message",0);
                   return 0;
}

//upto the caller to free the buffers in create_message_X cases

int create_message_REP_INSERT(struct op_code* op, char *message){

                   int key = op->key; 
                   char *value = op->value; 
                   int origin = op->owner; 
                   int friend1 = op->friend1; 
                   int friend2 = op->friend2;

                   funcEntry(logF,NULL,"create_message_REP_INSERT");
                   sprintf(message,"REP_INSERT:%d:%s:%d:%d:%d;",key,value,origin,friend1,friend2);
                   funcExit(logF,NULL,"create_message_REP_INSERT",0);
                   return 0;
}

int create_message_REP_DELETE(struct op_code* op,char *message){

                   int key = op->key;
                   char *value = op->value;
                   int origin = op->owner; 
                   int friend1 = op->friend1;
                   int friend2 = op->friend2;

                   funcEntry(logF,NULL,"create_message_REP_DELETE");
                   sprintf(message,"REP_DELETE:%d:%d:%d:%d;",key,origin,friend1,friend2);
                   funcExit(logF,NULL,"create_message_REP_DELETE",0);
                   return 0;
}

int create_message_REP_UPDATE(struct op_code* op, char *message){

                   int key = op->key;
                   char *value = op->value;
                   int origin = op->owner;
                   int friend1 = op->friend1;
                   int friend2 = op->friend2;

                   funcEntry(logF,NULL,"create_message_REP_UPDATE");
                   sprintf(message,"REP_UPDATE:%d:%s:%d:%d:%d;",key,value,origin,friend1,friend2);
                   funcExit(logF,NULL,"create_message_REP_UPDATE",0);
                   return 0;
}

int create_message_REP_LOOKUP(struct op_code* op, char *message){

                   int key = op->key;
                   char *value = op->value;
                   int origin = op->owner;
                   int friend1 = op->friend1;
                   int friend2 = op->friend2;

                   funcEntry(logF,NULL,"create_message_REP_LOOKUP");
                   sprintf(message,"REP_LOOKUP:%d:%d:%d:%d;",key,origin,friend1,friend2);
                   funcExit(logF,NULL,"create_message_REP_LOOKUP",0);
                   return 0;
}


int create_message_INSERT(int key, char *value, char *message){
                   funcEntry(logF,NULL,"create_message_INSERT");
	           int len = strlen(value);
	       //    char *buffer = (char *)malloc(300);
		   sprintf(message,"INSERT:%d:%s;",key,value);
		//   *message = buffer;
                   funcExit(logF,NULL,"create_message_INSERT",0);
		   return 0;
}
int create_message_INSERT_RESULT_SUCCESS(int key, char *message){
                   
                   funcEntry(logF,NULL,"create_message_RESULT_SUCCESS");
                //   char *buffer = (char *)malloc(300);
                   sprintf(message,"INSERT_RESULT_SUCCESS:%d;",key);
               //    *message = buffer;
                   funcExit(logF,NULL,"create_message_RESULT_SUCCESS",0);
                   return 0;
}
int create_message_DELETE(int key, char *message){
                   funcEntry(logF,NULL,"create_message_DELETE");
	        //   char *buffer = (char *)malloc(300);
	           sprintf(message,"DELETE:%d;",key);
		//   *message = buffer;
                   funcExit(logF,NULL,"create_message_DELETE",0);
		   return 0;
}
int create_message_DELETE_RESULT_SUCCESS(int key, char *message){
                   funcEntry(logF,NULL,"create_message_DELETE_RESULT_SUCCESS");
              //     char *buffer = (char *)malloc(300);
                   sprintf(message,"DELETE_RESULT_SUCCESS:%d;",key);
              //     *message = buffer;
                   funcExit(logF,NULL,"create_message_DELETE_RESULT_SUCCESS",0);
                   return 0;
}
int create_message_UPDATE(int key, char *value, char *message){
                   funcEntry(logF,NULL,"create_message_UPDATE");
	      //     int len = strlen(value);
	       //    char *buffer = (char *)malloc(len+20+100);
		   sprintf(message,"UPDATE:%d:%s;",key,value);
	      //	   *message = buffer;
                   funcExit(logF,NULL,"create_message_UPDATE",0);
		   return 0;
}
int create_message_UPDATE_RESULT_SUCCESS(int key, char *message){
                   funcEntry(logF,NULL,"create_message_UPDATE_RESULT_SUCCESS");
            //       char *buffer = (char *)malloc(300);
                   sprintf(message,"UPDATE_RESULT_SUCCESS:%d;",key);
           //        *message = buffer;
                   funcExit(logF,NULL,"create_message_UPDATE_RESULT_SUCCESS",0);
                   return 0;
}
int create_message_LOOKUP(int key, char *message){
                   funcEntry(logF,NULL,"create_message_LOOKUP");
	      //     char *buffer = (char *)malloc(300);
		   sprintf(message,"LOOKUP:%d;",key);
	      //	   *message = buffer;
                   funcExit(logF,NULL,"create_message_LOOKUP",0);
		   return 0;
}
int create_message_LOOKUP_RESULT(int key, char *value, char *message){
                   funcEntry(logF,NULL,"create_message_LOOKUP_RESULT");
	           int len = strlen(value);
	//	   char *buffer = (char *)malloc(len + 300);
		   sprintf(message,"LOOKUP_RESULT:%d:%s;",key,value);
	//	   message = buffer;
                   funcExit(logF,NULL,"create_message_LOOKUP_RESULT",0);
		   return 0;
}
int create_message_ERROR(char *message){
                   funcEntry(logF,NULL,"create_message_ERROR");
          //         char *buf = (char *)malloc(300);
                   strcpy(message,"ERROR:UNABLE TO COMPLETE THE REQUIRED OPERATION;");
          //         *message = buf;
                   funcExit(logF, NULL,"create_message_ERROR",0);
                   return 0;
}
int create_message_INSERT_LEAVE(int key, char *value, char *message){
                   funcEntry(logF,NULL,"create_message_INSERT_LEAVE");
           //        int len = strlen(value);
          //         char *buffer = (char *)malloc(len+300);
                   sprintf(message,"INSERT_LEAVE:%d:%s;",key,value);
           //        *message = buffer;
                   funcExit(logF, NULL, "create_message_INSERT_LEAVE",0);
                   return 0;
}


/*
struct op_code{
	     int opcode;
   	     int key;
 	     char *value;
};
*/
// INSERT, DELETE, UPDATE, GET messages are possible
int extract_message_op(char *message, struct op_code** instance){

                   funcEntry(logF,NULL,"extract_message_op");
                   char original[512];
	        //   char *original = (char *)malloc(strlen(message));
                   printToLog(logF,"printing message in extract_message",message);
                   if(strlen(message)==0){
                           printToLog(logF,"null message received","in extract_message");
                           return -1;
                   }

                   strcpy(original,message);
                   printToLog(logF,"extract_message_op: original",original);
                   // first extract the first part and then the second (port and the ip)

               //    char *another_copy = (char *)malloc(strlen(message));
                   char another_copy[512];
                   strcpy(another_copy,message);
                   printToLog(logF,"checking another copy, extract_message",another_copy);

                   char delim_temp[5]=";";
                   char *token1 = strtok(another_copy,delim_temp); // extract the first part
               //    char *token_on = (char *)malloc(strlen(token1));
                   char token_on[512];
                   strcpy(token_on,token1);                   

                   char *token2 = strtok(NULL,delim_temp);  // extract the second part
               //    char *ip_port = (char *)malloc(strlen(token2));
                   char ip_port[256];
                   strcpy(ip_port,token2);

                   char *third_part = strtok(NULL,delim_temp);  // extract the third part
                   char timestamp_conslevel[200];
                   strcpy(timestamp_conslevel,third_part);
                   char *timestamp = strtok(timestamp_conslevel,":");  // extract time stamp                  
                   int timestampval = atoi(timestamp);                    
                   char *conslevel = strtok(NULL,":");
                   int conslevelval = atoi(conslevel);

                   char *token3 = strtok(ip_port,":");   //extract port from 2nd part
                   char port[30];
		   strcpy(port,token3);

                   char *token4 = strtok(NULL,":");     //extract IP from 2nd part
                   char IP[30];                       // store to IP
                   strcpy(IP,token4);
             //      char port[30];                     // store to port
             //      strcpy(port,token3);

                   *instance = (struct op_code *)malloc(sizeof(struct op_code));    
                   
                   if(*instance == NULL){
                           printToLog(logF,"malloc failed in","extract_message");
                           return -1;  
                   }
                           
                   memset(*instance,0,sizeof(struct op_code));                 
             

                   strcpy((*instance)->port,port);
                   strcpy((*instance)->IP,IP); 

	 	 //  *instance = (struct op_code *)malloc(sizeof(struct op_code));  // up-to the caller to free this

		   char delim[5]=":";	
		   char *token=strtok(token_on,delim); 	
                    
		   if (strcmp(token,"INSERT")==0){   //INSERT:KEY:VALUE;
		            (*instance)->opcode = 1; // 1 is the op-code for insert
					
			    token=strtok(NULL,delim);  //GET KEY
                    	    (*instance)->key = atoi(token);  
 
                            token=strtok(NULL,delim);    //GET VALUE
			    int len = strlen(token);     
			    char *value_instance = (char *)malloc(len);
			    strcpy(value_instance,token);
                    	    (*instance)->value = value_instance;
                            (*instance)->timeStamp = timestampval;
                            (*instance)->cons_level = conslevelval;
                   			    	
                        //    free(ip_port);
                       //     free(token_on);
                        //    free(original);
                        //    free(another_copy);          

                            funcExit(logF,NULL,"extract_message_op",0);
                    	    return 1;
                   }
             					
		   if(strcmp(token,"DELETE")==0){  //DELETE:KEY;
		            (*instance)->opcode = 2; // 2 is the op-code for delete
			    token = strtok(NULL,delim);  // GET KEY
			    (*instance)->key = atoi(token);
			    (*instance)->value = NULL;
                            
                         //   free(token_on);
                         //   free(ip_port);
                         //   free(original);
                         //   free(another_copy);
                            
                            funcExit(logF,NULL,"extract_message_op",0); 
			    return 1;
	           }
		   if(strcmp(token,"UPDATE")==0){ //UPDATE:KEY:VALUE;
		            (*instance)->opcode = 3; // 3 is the op-code for update
                     	    token = strtok(NULL,delim); // GET KEY
                            (*instance)->key = atoi(token);
            
                            token = strtok(NULL,delim);  // get the value to update
                            int len = strlen(token);
                            char *value_instance = (char *)malloc(len);					 
			    strcpy(value_instance, token);
			    (*instance)->value = value_instance;
                            
                      //      free(token_on);
                      //      free(ip_port);
                      //      free(original);
                      //      free(another_copy);         
 
                            funcExit(logF,NULL,"extract_message_op",0);
			    return 1;
		   }
		   if(strcmp(token,"LOOKUP")==0){  //LOOKUP:KEY;
		            (*instance)->opcode = 4; // 4 is the opcode for lookup
			    token = strtok(NULL,delim); //get key
			    (*instance)->key = atoi(token);
                            (*instance)->value = NULL;
                            
		      //      free(token_on);
                      //      free(ip_port);		
                      //      free(original);
                      //      free(another_copy);                     
       
		            funcExit(logF,NULL,"extract_message_op",0);		
			    return 1;
		   }
		   if(strcmp(token,"LOOKUP_RESULT")==0){
			    (*instance)->opcode = 5; // 5 is the opcode for lookup result
		            token = strtok(NULL,delim); //get key
		            (*instance)->key = atoi(token);
					 
			    token = strtok(NULL,delim); // get value
			    int len = strlen(token);
			    char *value_instance = (char *)malloc(len);
			    strcpy(value_instance, token);
			    (*instance)->value = value_instance;
                            
                    //        free(token_on);
                    //        free(ip_port);
                    //        free(original);
                    //        free(another_copy);                             

 			    funcExit(logF,NULL,"extract_message_op",0);
                            return 1;
		   }
                   if(strcmp(token,"ERROR")==0){
                            (*instance)->opcode = 99; // 99 is the error message opcode
                            token = strtok(NULL,delim); //get error message
                            char *value_instance = (char *)malloc(strlen(token));
                            strcpy(value_instance,token);
                            (*instance)->value = value_instance;
                            
                     //       free(token_on);
                     //       free(ip_port);
                     //       free(original);
                     //       free(another_copy);

			    funcExit(logF,NULL,"extract_message_op",0);	
                            return 1;
                   }
                   if(strcmp(token,"INSERT_LEAVE")==0){
                            (*instance)->opcode = 9; // 1 is the op-code for insert

                            token=strtok(NULL,delim);  //GET KEY
                            (*instance)->key = atoi(token);

                            token=strtok(NULL,delim);    //GET VALUE
                            int len = strlen(token);
                            char *value_instance = (char *)malloc(len);
                            strcpy(value_instance,token);

                            (*instance)->value = value_instance;
                            (*instance)->timeStamp = timestampval;
                            (*instance)->cons_level = conslevelval;

			//    free(token_on);
                        //    free(ip_port);	
                        //    free(original);
                        //    free(another_copy);

                            funcExit(logF,NULL,"extract_message_op",0);
                            return 1;
                   }


                  if(strcmp(token,"REP_INSERT")==0){
                            (*instance)->opcode = 10; // opcode for INSERT_REP
                            token = strtok(NULL,delim); // get key
                            (*instance)->key = atoi(token);
                            
                            token = strtok(NULL,delim);  // get value
                            int len = strlen(token);

                            char *value_instance = (char *)malloc(len);
                            strcpy(value_instance,token);
 
 		            (*instance)->value = value_instance;
                            (*instance)->timeStamp = timestampval;
                            (*instance)->cons_level = conslevelval;
             
                            token = strtok(NULL, delim);  // get owner                            
                            (*instance)->owner = atoi(token);
                         
                            token = strtok(NULL, delim);  // get friend1
		            (*instance)->friend1 = atoi(token);

                            token = strtok(NULL, delim);  // get friend2
                            (*instance)->friend2 = atoi(token);
                            
                        //    free(token_on);
                        //    free(ip_port);
                        //    free(original);
                        //    free(another_copy);

                            funcExit(logF,NULL,"extract_message_op",0);
                            return 1;
                  }

                  if(strcmp(token,"REP_DELETE")==0){
                            (*instance)->opcode = 11; // opcode for delete_rep
                            token = strtok(NULL,delim);   // get key
                            (*instance)->key = atoi(token);

                            (*instance)->timeStamp = timestampval;
                            (*instance)->cons_level = conslevelval;

                            token = strtok(NULL, delim);  // get owner                            
                            (*instance)->owner = atoi(token);

                            token = strtok(NULL, delim);  // get friend1
                            (*instance)->friend1 = atoi(token);

                            token = strtok(NULL, delim);  // get friend2
                            (*instance)->friend2 = atoi(token);

                       //     free(token_on);
                       //     free(ip_port);
                       //     free(original);
                       //     free(another_copy);

                            funcExit(logF,NULL,"extract_message_op",0);
                            return 1;
                  }
 
                  if(strcmp(token,"REP_LOOKUP")==0){
                            (*instance)->opcode = 13; // opcode for delete_rep
                            token = strtok(NULL,delim);   // get key
                            (*instance)->key = atoi(token);

                            (*instance)->timeStamp = timestampval;
                            (*instance)->cons_level = conslevelval;

                            token = strtok(NULL, delim);  // get owner                            
                            (*instance)->owner = atoi(token);

                            token = strtok(NULL, delim);  // get friend1
                            (*instance)->friend1 = atoi(token);

                            token = strtok(NULL, delim);  // get friend2
                            (*instance)->friend2 = atoi(token);

                       //     free(token_on);
                       //     free(ip_port);
                       //     free(original);
                       //     free(another_copy);

                            funcExit(logF,NULL,"extract_message_op",0);
                            return 1;
                  }  

                  if(strcmp(token,"REP_UPDATE")==0){

                            (*instance)->opcode = 12; // opcode for UPDATE_REP
                            token = strtok(NULL,delim); // get key
                            (*instance)->key = atoi(token);

                            token = strtok(NULL,delim);  // get value
                            int len = strlen(token);

                            char *value_instance = (char *)malloc(len);
                            strcpy(value_instance,token);

                            (*instance)->value = value_instance;
                            (*instance)->timeStamp = timestampval;
                            (*instance)->cons_level = conslevelval;

                            token = strtok(NULL, delim);  // get owner                            
                            (*instance)->owner = atoi(token);

                            token = strtok(NULL, delim);  // get friend1
                            (*instance)->friend1 = atoi(token);

                            token = strtok(NULL, delim);  // get friend2
                            (*instance)->friend2 = atoi(token);

                       //     free(token_on);
                       //     free(ip_port);
                       //     free(original);
                       //     free(another_copy);

                            funcExit(logF,NULL,"extract_message_op",0);
                            return 1; 
                  }  

                                
                   if(strcmp(token,"INSERT_RESULT_SUCCESS")==0){
                            printToLog(logF,"In INSERT_RESULT_SUCCESS","HERE");
                            funcExit(logF,NULL,"extract_message_op",0);
                            (*instance)->opcode = 6;
                        //    free(original); 
                        //    free(another_copy);  
                        //    free(token_on); 
                        //    free(ip_port);  
                            return 6;
                   }
                   if(strcmp(token,"DELETE_RESULT_SUCCESS")==0){
                            funcExit(logF,NULL,"extract_message_op",0);
                            (*instance)->opcode = 7;
                        //    free(original); 
                        //    free(another_copy);  
                        //    free(token_on); 
                         //      free(ip_port);  
                            return 7;
                    }
                   if(strcmp(token,"UPDATE_RESULT_SUCCESS")==0){
                            funcExit(logF,NULL,"extract_message_op",0);
                            (*instance)->opcode = 8;
                       //     free(original); 
                       //     free(another_copy);  
                       //     free(token_on); 
                       //     free(ip_port);  
                            return 8;
                   }
    
}
			

// replace host_id to hash_id of the host
/*
void main(){

   int   i_rc = logFileCreate(LOG_FILE_LOCATION);
     if ( i_rc != SUCCESS )
        {
          printf("\nLog file won't be created. There was an error\n");
          int rc = -1;
         // goto rtn;
         }
  // create_message_XXXX examples
   struct op_code *temp=0x0;
   char *msg=0x0;
   char value[100] = "192.145.1.uselessfellowbloodyfellownonsense";
   int key = 100;
   int key1 = g_str_hash(value);
   printf("hash value for hello world is %d\n",key1%360);
   int i=0;

   while(i<1000){
    create_message_LOOKUP_RESULT(key,value,&msg);
    append_port_ip_to_message("1234","192.168.1.1",msg);
    printf("%s\n",msg);
    free(msg);
    i++;
   }

   msg=0x0;
   create_message_LOOKUP_RESULT(key,value,&msg);
   append_port_ip_to_message("1234","192.168.100.100",msg);

   //struct op_code *temp;
   i = 0;
   int n;
   while(i<100){
    temp=0x0;
    n = extract_message_op(msg,&temp);
    printf("%d\n",n);

    printf("key : %d\n",temp->key);
    printf("value : %s\n",temp->value);
    printf("opcode:%d\n",temp->opcode);
    printf("IP : %s\n",temp->IP);
    printf("Port : %s\n",temp->port);
    i++;
    free(temp);
   }
*/
/*   
   create_message_DELETE(1234,&msg);
   printf("%s\n",msg);
   free(msg);
   msg=0x0;

   create_message_UPDATE(1234,value,&msg);
   printf("%s\n",msg);
   free(msg);
   msg=0x0;

   create_message_LOOKUP(1234,&msg);
   printf("%s\n",msg);
   free(msg);
   msg=0x0;


   create_message_LOOKUP_RESULT(1234,value,&msg);
   printf("%s\n",msg);
   free(msg);
   msg=0x0; 
*/  
//   struct op_code *temp;
  // extract_message examples
/*
int m=0;

   printf("-------%d----------------\n",m);
   create_message_INSERT(100,value,&msg);
   temp=0x0;
 //  struct op_code* temp;
   extract_message_op(msg,&temp);
   printf("%s\n",msg);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   printf("-------------------------\n");
   msg=0x0;
   m++;

   msg=0x0;
   temp=0x0;
   create_message_DELETE(1234,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   printf("-------------------------\n");
   msg=0x0;

   create_message_UPDATE(1234,value,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;
   printf("--------------------------\n");


   create_message_LOOKUP(1234,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;
   printf("--------------------------\n");

   
   create_message_LOOKUP_RESULT(1234,value,&msg);
   extract_message_op(msg,&temp);
   printf("%s\n",msg);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;
   
   m++;  

*/

/*
   temp=NULL;
   char *msg1=NULL;
   printf("------last lookup--------------\n");
   create_message_LOOKUP(1234,&msg1);
   extract_message_op(msg1,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg1);
   free(temp);
   msg1=0x0;
*/


/*
   create_message_LOOKUP(1234,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;

   create_message_LOOKUP(1234,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;

   
   create_message_DELETE(1234,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;

   create_message_DELETE(1234,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;

   create_message_DELETE(1234,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;

   create_message_DELETE(1234,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;

   create_message_DELETE(1234,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;
*/
/*  
   create_message_LOOKUP_RESULT(1234,value,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
 //  free(msg);
   free(temp);
   printf("\n---------------------------\n");
   msg=0x0;

   create_message_LOOKUP_RESULT(1234,value,&msg);
   extract_message_op(msg,&temp);
   printf("%s\n",msg);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
 //  free(msg);
   free(temp);
   msg=0x0;  
   
   char *msg1;
   create_message_LOOKUP_RESULT(1234,value,&msg1);
   extract_message_op(msg1,&temp);
   printf("%s\n",msg1);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
//   free(msg1);
   free(temp);
    
   char *msg2;
   create_message_LOOKUP_RESULT(1234,value,&msg2);
   extract_message_op(msg2,&temp);
   printf("%s\n",msg2);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
 //  free(msg2);

   free(temp);
*/
/*
   msg=0x0;
   
   char *msg3;
   create_message_DELETE(1234,&msg3);
   printf("%s",msg3);
   extract_message_op(msg3,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg3);
   free(temp);

   msg=0x0;
   create_message_INSERT(100,value,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;

   create_message_INSERT(100,value,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;

   create_message_INSERT(100,value,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;
  
   create_message_INSERT(100,value,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;


   create_message_UPDATE(1234,value,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;

   create_message_UPDATE(1234,value,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;

   create_message_UPDATE(1234,value,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;

   create_message_UPDATE(1234,value,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;

   create_message_UPDATE(1234,value,&msg);
   extract_message_op(msg,&temp);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
   free(msg);
   free(temp);
   msg=0x0;
*/
/*
   create_message_LOOKUP_RESULT(1234,value,&msg2);
   char *buffer=(char *)malloc(100);
   strcpy(buffer,msg2);
   printf("printing from buffer.........%s\n",buffer);
   free(buffer);
   extract_message_op(msg2,&temp);
   printf("%s\n",msg2);
   printf("key : %d\n",temp->key);
   printf("value : %s\n",temp->value);
   printf("opcode:%d\n",temp->opcode);
 //  free(msg2);
*/
   
/*
   printf("===================================================\n");
   printf("====================hash table creation============\n");

   create_message_LOOKUP_RESULT(123,value,&msg);
   append_port_ip_to_message("1234","192.168.100.100",msg);
   extract_message_op(msg,&temp);
   create_hash_table();

   insert_key_value_into_store(temp);
   char *value123 = lookup_store_for_key(123);

   printf("%s\n",value123);
  
   temp->key = 3456;
   temp->value = "GRK";
   insert_key_value_into_store(temp);
   value123 = lookup_store_for_key(3456);
   printf("%s\n",value123);
  
   printf("----------iterating hash table--------\n");
   iterate_hash_table();
}          	
*/		   
 
        
