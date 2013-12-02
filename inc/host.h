//////////////////////////////////////////////////////////////////////////////
//****************************************************************************
//
//    FILE NAME: host.h 
//
//    DECSRIPTION: This is the header file for the leader
//                 host and the member hosts  
//
//    OPERATING SYSTEM: Linux UNIX only
//    TESTED ON:
//
//    CHANGE ACTIVITY:
//    Date        Who      Description
//    ==========  =======  ===============
//    09-29-2013  Rajath   Initial creation
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

/*
 * Macros
 */
#define SUCCESS                   0
#define ERROR                     -1
#define NUM_OF_CL_ARGS            5 
#define LEADER                    7
#define MEMBER                    8
#define NUM_OF_THREADS            6   
#define JOIN_OP_CODE              9
#define RECEIVE_HB_OP_CODE        8
#define GOSSIP_HOSTS              2
#define NEW_INCARNATION           1
#define REINCARNATION             2
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
#define INSERT_LEAVE_KV           9
#define REP_INSERT                10
#define REP_DELETE                11
#define REP_UPDATE                12
#define REP_LOOKUP                13
#define ERROR_RESULT              99
#define PRINT_KV                  5
#define REORDER_CHECK_TIME_PERIOD 25
#define NUM_OF_FRIENDS            2
#define LEADER_STRING             "leader"
#define MEMBER_STRING             "member"

/*
 * Global variables
 */
//FILE *logF;                            // File pointer to log
int udp;                               // UDP socket descriptor
int tcp;                               // TCP socket descriptor 
struct sockaddr_in hostAddress;        // Host address
char ipAddress[SMALL_BUF_SZ],          // IP of current host
     portNo[SMALL_BUF_SZ],             // Port no of current host
     logMsg[MED_BUF_SZ];               // Log message buffer 
bool isLeader = false;                 // Bool variable
//extern struct hb_entry hb_table[MAX_HOSTS];

/* 
 * Function Declarations
 */
int CLA_checker(
                int argc,        // Number of CLA
                char *argv[]     // CLAs
                );
int setUpPorts();
int requestMembershipToLeader(
                              int leaderPort,     // Leader port
                              char *leaderIp      // Leader IP 
                             );
int CLI_UI();
int askLeaderIfRejoinOrNew();
int spawnHelperThreads();
void * startKelsa(void *);
int receiverFunc();
int sendFunc();
int heartBeatCheckerFunc();
int checkOperationCode(
                       char * recMsg,        // Received message
                       int * op_code,        // Returned op code
                       char * tokenRecMsg    // Returned msg
                      );
void leaveSystem(
                 int signum            // Signal
                );
int intialize_local_key_value_store();
//int prepareNodeForSystemLeave();
//int sendKVFunc();
int receiveKVFunc();
int localKVReorderFunc();
int printKVStore();
//int replicateKV(struct op_code * op_instance, int * friendListPtr);
void * FEfunction(void *);

/*
 * End 
 */
