//////////////////////////////////////////////////////////////////////////////
//****************************************************************************
//
//    FILE NAME: KVclient.h 
//
//    DECSRIPTION: This is the header file for the client of the 
//                 key-value store
//
//    OPERATING SYSTEM: Linux UNIX only
//    TESTED ON:
//
//    CHANGE ACTIVITY:
//    Date        Who      Description
//    ==========  =======  ===============
//    11-09-2013  Rajath   Initial creation
//
//****************************************************************************
//////////////////////////////////////////////////////////////////////////////

/*
 * Header files
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <stdbool.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <pthread.h>
#include <sys/time.h>
#include <signal.h>
#include "../src/logger.c"

/*
 * Macros
 */
#define SUCCESS                   0
#define ERROR                     -1
#define CLIENT_NUM_OF_CL_ARGS     6 
#define NUM_OF_THREADS            2
#define SMALL_BUF_SZ              100
#define MED_BUF_SZ                1024
#define LONG_BUF_SZ               4096
#define INSERT_KV                 1
#define LOOKUP_KV                 4 
#define UPDATE_KV                 3 
#define DELETE_KV                 2 
#define LOOKUP_RESULT             5
#define INSERT_RESULT             6
#define DELETE_RESULT             7
#define UPDATE_RESULT             8
#define ERROR_RESULT              99

/*
 * Global variables
 */
int tcp;
char clientPortNo[SMALL_BUF_SZ];
char clientIpAddr[SMALL_BUF_SZ];
char serverPortNo[SMALL_BUF_SZ];
char KVclientCmd[LONG_BUF_SZ];
int consistencyLevel;
char opCode[SMALL_BUF_SZ];
char key[SMALL_BUF_SZ];
char value[LONG_BUF_SZ];
char msgToSend[LONG_BUF_SZ];
char ipAddress[SMALL_BUF_SZ];
char logMsg[LONG_BUF_SZ];
struct sockaddr_in KVClientAddr;

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


/*
 * Function Declarations
 */
int KVClient_CLA_check(int argc, char *argv[]);
int setUpTCP(char * portNo, char * ipAddress);
int spawnHelperThreads();
void * startKelsa(void *threadNum);
int clientReceiveFunc();
int clientSenderFunc();
int parseKVClientCmd();
int createAndSendOpMsg();

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
                   strcpy(original,message);

                   // first extract the first part and then the second (port and the ip)

               //    char *another_copy = (char *)malloc(strlen(message));
                   char another_copy[512];
                   strcpy(another_copy,message);

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
                            
                        //    free(original); 
                        //    free(another_copy);  
                        //    free(token_on); 
                        //    free(ip_port);  
                            return 6;
                   }
                   if(strcmp(token,"DELETE_RESULT_SUCCESS")==0){
                            funcExit(logF,NULL,"extract_message_op",0);
                        //    free(original); 
                        //    free(another_copy);  
                        //    free(token_on); 
                        //      free(ip_port);  
                            return 7;
                    }
                   if(strcmp(token,"UPDATE_RESULT_SUCCESS")==0){
                            funcExit(logF,NULL,"extract_message_op",0);
                       //     free(original); 
                       //     free(another_copy);  
                       //     free(token_on); 
                       //     free(ip_port);  
                            return 8;
                   }
    
}
			


/*
 * END
 */
