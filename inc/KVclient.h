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
#define CLIENT_NUM_OF_CL_ARGS     5 
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

                   if(strcmp(token,"INSERT_RESULT_SUCCESS")==0){funcExit(logF,NULL,"extract_message_op",0); free(original); free(another_copy); return 6;}
                   if(strcmp(token,"DELETE_RESULT_SUCCESS")==0){funcExit(logF,NULL,"extract_message_op",0); free(original); free(another_copy); return 7;}
                   if(strcmp(token,"UPDATE_RESULT_SUCCESS")==0){funcExit(logF,NULL,"extract_message_op",0); free(original); free(another_copy); return 8;}

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
               //    int len = strlen(value);
               //    char *buffer = (char *)malloc(300);
                   sprintf(message,"INSERT:%d:%s;",key,value);
              //     *message = buffer;
                   funcExit(logF,NULL,"create_message_INSERT",0);
                   return 0;
}
int create_message_DELETE(int key, char *message){
                   funcEntry(logF,NULL,"create_message_DELETE");
              //     char *buffer = (char *)malloc(300);
                   sprintf(message,"DELETE:%d;",key);
              //     *message = buffer;
                   funcExit(logF,NULL,"create_message_DELETE",0);
                   return 0;
}
int create_message_UPDATE(int key, char *value, char *message){
                   funcEntry(logF,NULL,"create_message_UPDATE");
            //       int len = strlen(value);
            //       char *buffer = (char *)malloc(len+20+100);
                   sprintf(message,"UPDATE:%d:%s;",key,value);
            //       *message = buffer;
                   funcExit(logF,NULL,"create_message_UPDATE",0);
                   return 0;
}
int create_message_LOOKUP(int key, char *message){
                   funcEntry(logF,NULL,"create_message_LOOKUP");
         //          char *buffer = (char *)malloc(300);
                   sprintf(message,"LOOKUP:%d;",key);
         //          *message = buffer;
                   funcExit(logF,NULL,"create_message_LOOKUP",0);
                   return 0;
}
int create_message_ERROR(char *message){
                   funcEntry(logF,NULL,"create_message_ERROR");
           //        char *buf = (char *)malloc(300);
                   strcpy(message,"ERROR:UNABLE TO COMPLETE THE REQUIRED OPERATION;");
           //        *message = buf;
                   funcExit(logF, NULL,"create_message_ERROR",0);
                   return 0;
}



/*
 * END
 */
