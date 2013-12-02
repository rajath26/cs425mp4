//////////////////////////////////////////////////////////////////////////////
//****************************************************************************
//
//    FILE NAME: tcp.c
//
//    DECSRIPTION: This is the source file for UDP functionality  
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
#include "../inc/tcp.h"

/*
 * Function declarations
 */

/*****************************************************************
 * NAME: recvTCP
 *
 * DESCRIPTION: This function is wrapper over recv 
 *              
 * PARAMETERS: 
 * (char *) buffer - buffer received to be sent back 
 * (int) length - max length of buffer received 
 * (struct sockaddr_in) hostAddr - struct holding address of 
 *                                 host buffer received from
 * (int *) ret_tcp - pass by reference new socket descriptor
 *                   returned by accept
 * (int) acceptOrNot - integer to tell whether to accept or not
 *
 * RETURN:
 * (int) bytes received 
 * 
 ****************************************************************/
int recvTCP(int sd, char * buffer, int length)
{

    funcEntry(logF, ipAddress, "recvTCP");

    int numOfBytesRec;       // Number of bytes received

    numOfBytesRec = recv(sd, buffer, length, 0);

  rtn:
    funcExit(logF, ipAddress, "recvTCP", numOfBytesRec);
    return numOfBytesRec;

} // End of recvTCP()

/*****************************************************************
 * NAME: sendTCP
 *
 * DESCRIPTION: This function is wrapper over send 
 *              
 * PARAMETERS: 
 * (int) portNo - port no
 * (char *) ipAddr - IP 
 * (char *) buffer - buffer to be sent 
 * (int) new_tcp - new TCP sd returned by accept
 * 
 * RETURN:
 * (int) bytes sent 
 * 
 ****************************************************************/
int sendTCP(int sd, char *buffer, int length)
{

    funcEntry(logF, ipAddress, "sendTCP");

    int numOfBytesSent;               // Number of bytes sent 

    //host_to_network(buffer);
    numOfBytesSent = send(sd, buffer, strlen(buffer), 0);

rtn:
    funcExit(logF, ipAddress, "sendTCP", numOfBytesSent);
    //close(sd);
    return numOfBytesSent; 

} // End of sendTCP()

/*
 * End
 */
