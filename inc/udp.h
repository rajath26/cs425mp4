//////////////////////////////////////////////////////////////////////////////
//****************************************************************************
//
//    FILE NAME: udp.h 
//
//    DECSRIPTION: This is the header file for UDP functionality  
//
//    OPERATING SYSTEM: Linux UNIX only
//    TESTED ON:
//
//    CHANGE ACTIVITY:
//    Date        Who      Description
//    ==========  =======  ===============
//    10-01-2013  Rajath   Initial creation
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
#define SMALL_BUF_SIZE 100

/* 
 * Global variables
 */
extern char ipAddress[SMALL_BUF_SIZE];
extern char portNo[SMALL_BUF_SIZE];

/*  
 * Function declarations
 */
int recvUDP(
            char *buffer,        // Buffer to be received  
            int length,          // Length of buffer 
            struct sockaddr_in   // Struct holding address
           );
int sendUDP(
            int portNo,          // Port No
            char * ipAddr,       // IP Address, 
            char * buffer        // Buffer to be sent
           );

/*
 * End
 */
