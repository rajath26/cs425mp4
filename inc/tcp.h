//////////////////////////////////////////////////////////////////////////////
//****************************************************************************
//
//    FILE NAME: tcp.h 
//
//    DECSRIPTION: This is the header file for TCP functionality  
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
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <sys/time.h>
#include "logger.h"

/*
 * Macros
 */
#define SMALL_BUF_SIZE      100
#define LISTEN_QUEUE_LENGTH 10

/* 
 * Global variables
 */
extern char ipAddress[SMALL_BUF_SIZE];
extern char portNo[SMALL_BUF_SIZE];
//char logMsg[LONG_BUF_SZ];

/*  
 * Function declarations
 */
int recvTCP(
            int sd,
            char *buffer, 
            int length
           );
int sendTCP(
            int sd, 
            char *buffer, 
            int length
           );

/*
 * End
 */
