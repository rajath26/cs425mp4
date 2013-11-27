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


struct op_code{
             int opcode;
             int key;
             char *value;
             char port[15];
             char IP[40];
};


GQueue* temp_list=0x0;
GHashTable* key_value_store;

pthread_mutex_t key_value_mutex;


int prepare_system_for_leave(gpointer key,gpointer value, gpointer dummy){
         funcEntry(logF,NULL,"prepare_system_for_leave");
       //  int i = choose_host_hb_index(atoi((char*)key));
         int rc = 1;
         char port[20];
         char IP[100];
         char response[4096];
         int sd;
         int i_rc;
         struct sockaddr_in peer;
         guint m = g_hash_table_size(key_value_store);
         char message[4096];
         if(m!=0){
               update_host_list();
               int i = choose_host_hb_index(atoi((char*)key));
               memset(message, '\0', 4096);
               create_message_INSERT_LEAVE(atoi((char *)key),(char *)value,message);
               append_port_ip_to_message(hb_table[host_no].port,hb_table[host_no].IP,message);
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
               int numOfBytesSent = sendTCP(sd, message, sizeof(message));
               if ( 0 == numOfBytesSent )
               {
                   printf("\nZERO BYTES SENT IN prepare_system_for_leave\n");
                   rc = 0;
                   goto rtn;
               }
               int numOfBytesRec = recvTCP(sd, response, 4096);
               if ( 0 == numOfBytesRec )
               {
                   printf("\nZERO BYTES RECEIVED IN prepare_system_for_leave\n");
                   rc = 0;
                   goto rtn;
               }
               //delete_key_value_from_store(atoi((char *)key));
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
         update_host_list();
         guint m = g_hash_table_size(key_value_store);
         sprintf(logMsg, "The Hash table size during leaveSystem is : %d", (int)m);
         printToLog(logF, "THE HASH TABLE SIZE DURING LEAVESYSTEM IS", logMsg);
         printf("\n%s\n", logMsg);
          if(m==0)return;
         g_hash_table_foreach_remove(key_value_store,prepare_system_for_leave,NULL);
         pthread_mutex_unlock(&table_mutex);
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
         pthread_mutex_lock(&key_value_mutex);
         printf("\n\t*******************************\n");
         printf("\tKEY : %s - VALUE : %s\n",(char *)key,(char *)value);
         printf("\n\t*******************************\n");
          
         pthread_mutex_unlock(&key_value_mutex);
         funcExit(logF,NULL,"print_key_value",0);
}

void process_key_value(gpointer key,gpointer value, gpointer dummy){
	 funcEntry(logF,NULL,"process_key_value");
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
         //struct sockaddr_in address;
         struct op_code *temp = NULL;
         strcpy(port,hb_table[i].port);
         strcpy(IP,hb_table[i].IP);
          
          guint m = g_hash_table_size(key_value_store);
          if(m==0)return;
        
         // delete_key_value_from_store(atoi((char *)key));
         if(i!=host_no){
             memset(message, '\0', 4096);
             create_message_INSERT(atoi((char *)key),(char *)value,message);
             sprintf(logMsg, "PORT: %s, IP : %s , message: %s", hb_table[host_no].port, hb_table[host_no].port, message);
             printToLog(logF, "PROCESS_KEY_VALUE", logMsg);
             append_port_ip_to_message(hb_table[host_no].port,hb_table[host_no].IP,message);         
             //sendKV:
               sd = socket(AF_INET, SOCK_STREAM, 0);
               if ( -1 == sd )
               {
                    printf("\nUnable to open socket in prepare_system_for_leave\n");
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
                   printf("\nCant connect to server in prepare_system_for_leave\n");
                   goto rtn;
               }
               int numOfBytesSent = sendTCP(sd, message, sizeof(message));
               if ( 0 == numOfBytesSent )
               {
                   printf("\nZERO BYTES SENT IN prepare_system_for_leave\n");
                   goto rtn;
               }
               int numOfBytesRec = recvTCP(sd, response, 4096);
               if ( 0 == numOfBytesRec )
               {
                   printf("\nZERO BYTES RECEIVED IN prepare_system_for_leave\n");
                   goto rtn;
               }
            delete_key_value_from_store(atoi((char *)key));
           //funcExit(logF,NULL,"process_key_value",0);
         }

       rtn:
        close(sd);
        funcExit(logF,NULL,"process_key_value",0);
}
void reorganize_key_value_store(){
         funcEntry(logF,NULL,"reorganize_key_value_store");
         if (systemIsLeaving)
             return;
         guint m = g_hash_table_size(key_value_store);
         if(m==0) return;
         g_hash_table_foreach(key_value_store,process_key_value,NULL);
         reOrderTrigger=0;         
         funcExit(logF,NULL,"reorganize_key_value_store",0);
}

void iterate_hash_table(){
         funcEntry(logF,NULL,"iterate_hash_table");
//         pthread_mutex_lock(&key_value_mutex);
         guint size = g_hash_table_size(key_value_store);
         printf("\nHASH TABLE SIZE: %d\n", (int)size);
         g_hash_table_foreach(key_value_store,print_key_value,NULL);
//         pthread_mutex_unlock(&key_value_mutex);
         funcExit(logF,NULL,"iterate_hash_table",0);
}
/*
void print_key_value(char *key,char *value){
         printf("key :%s, value : %s\n",key,value);
}*/

int create_hash_table(){
   funcEntry(logF,NULL,"create_hash_table");
   pthread_mutex_lock(&key_value_mutex);
   key_value_store =  g_hash_table_new(g_str_hash,g_str_equal);
   if(key_value_store == NULL){
             pthread_mutex_unlock(&key_value_mutex); 
             funcExit(logF,NULL,"create_hash_table",0);
             return -1;
   }
   else{
             pthread_mutex_unlock(&key_value_mutex); 
             funcExit(logF,NULL,"create_hash_table",0);
             return 0;
   }
}

// send an opcode instance which is dynamically allocated
//
int insert_key_value_into_store(struct op_code* op_instance){
   
     funcEntry(logF,NULL,"insert_key_value_into_store");  
     pthread_mutex_lock(&key_value_mutex);
     char *buffer;
     buffer = (char*)malloc(200);
     sprintf(buffer,"%d",op_instance->key);

     gpointer key = (gpointer)buffer;
     gpointer value = (gpointer)op_instance->value;

     g_hash_table_insert(key_value_store,key,value);
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
     free(buffer);
     pthread_mutex_unlock(&key_value_mutex);
     funcExit(logF,NULL,"lookup_store_for_key",0);
     return (char *)value;
}

int update_key_value_in_store(struct op_code *op_instance)
{
    funcEntry(logF, NULL, "update_key_value_into_store");
    int rc = 0,i_rc;
    char *lookupValue = lookup_store_for_key(op_instance->key);
    if ( NULL == lookupValue ) {
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
          
          free((char *)value);   // free_code
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
	           char *original = (char *)malloc(strlen(message));
                   strcpy(original,message);

                   // first extract the first part and then the second (port and the ip)

                   char *another_copy = (char *)malloc(strlen(message));
                   strcpy(another_copy,message);

                   char delim_temp[5]=";";
                   char *token1 = strtok(another_copy,delim_temp); // extract the first part
                   char *token_on = (char *)malloc(strlen(token1));
                   strcpy(token_on,token1);                   

                   char *token2 = strtok(NULL,delim_temp);  // extract the second part

                   char *ip_port = (char *)malloc(strlen(token2));
                   strcpy(ip_port,token2);

                   char *token3 = strtok(ip_port,":");   //extract port from 2nd part
                   char *token4 = strtok(NULL,":");     //extract IP from 2nd part
                   char IP[30];                       // store to IP
                   strcpy(IP,token4);
                   char port[10];                     // store to port
                   strcpy(port,token3);

                   *instance = (struct op_code *)malloc(sizeof(struct op_code));    
                 
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
             
                            free(original);
                            free(another_copy);          

                            funcExit(logF,NULL,"extract_message_op",0);
                    	    return 1;
                   }
             					
		   if(strcmp(token,"DELETE")==0){  //DELETE:KEY;
		            (*instance)->opcode = 2; // 2 is the op-code for delete
			    token = strtok(NULL,delim);  // GET KEY
			    (*instance)->key = atoi(token);
			    (*instance)->value = NULL;
                            
                            free(original);
                            free(another_copy);
                            
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
                            
                            free(original);
                            free(another_copy);         
 
                            funcExit(logF,NULL,"extract_message_op",0);
			    return 1;
		   }
		   if(strcmp(token,"LOOKUP")==0){  //LOOKUP:KEY;
		            (*instance)->opcode = 4; // 4 is the opcode for lookup
			    token = strtok(NULL,delim); //get key
			    (*instance)->key = atoi(token);
                            (*instance)->value = NULL;
                            
                            free(original);
                            free(another_copy);                     
       
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
                            
                            free(original);
                            free(another_copy);                             

 			    funcExit(logF,NULL,"extract_message_op",0);
                            return 1;
		   }
                   if(strcmp(token,"ERROR")==0){
                            (*instance)->opcode = 99; // 99 is the error message opcode
                            token = strtok(NULL,delim); //get error message
                            char *value_instance = (char *)malloc(strlen(token));
                            strcpy(value_instance,token);
                            (*instance)->value = value_instance;
                            
                            free(original);
                            free(another_copy);

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

                            free(original);
                            free(another_copy);

                            funcExit(logF,NULL,"extract_message_op",0);
                            return 1;
                   }
                                               
                   if(strcmp(token,"INSERT_RESULT_SUCCESS")==0){funcExit(logF,NULL,"extract_message_op",0);free(original); free(another_copy);    return 6;}
                   if(strcmp(token,"DELETE_RESULT_SUCCESS")==0){funcExit(logF,NULL,"extract_message_op",0);free(original); free(another_copy);    return 7;}
                   if(strcmp(token,"UPDATE_RESULT_SUCCESS")==0){funcExit(logF,NULL,"extract_message_op",0);free(original); free(another_copy);    return 8;}
    
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
 
        
