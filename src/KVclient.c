/////////////////////////////////////////////////////////////////////////////
//****************************************************************************
//
//    FILE NAME: KVclient.c
//
//    DECSRIPTION: This is the source file for the client of the 
//                 key value store 
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

#include "../inc/KVclient.h"
//#include "key_value_store.c"
#include "tcp.c"
//#include "../inc/logger.h"

/* 
 * Function defintions
 */ 

/*****************************************************************
 * NAME: KVClient_CLA_check
 *
 * DESCRIPTION: This function is designed to check if command 
 *              line arguments is valid 
 *              
 * PARAMETERS: 
 *            (int) argc - number of command line arguments
 *            (char *) argv - three command line arguments apart 
 *                            from argv[0] namely:
 *                            i) port no
 *                            ii) Ip Address of host
 *                            iii) port no of server 
 *                            iv) KV Client Command
 *                            v) Consistency Level
 * 
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int KVClient_CLA_check(int argc, char *argv[])
{

    funcEntry(logF, ipAddress, "KVClient_CLA_check");

    int rc = SUCCESS;

    if ( argc != CLIENT_NUM_OF_CL_ARGS )
    {
        printf("\nInvalid usage\n");
        printf("\nUsage information: %s <port_no> <ip_address> <server_port> <KV_client_command> <consistency_level>\n", argv[0]);
        printf("\t\t-> KV Functionalities / OP_CODE\n");
        printf("\t\ti) INSERT\n"); 
        printf("\t\tii) LOOKUP\n");
        printf("\t\tiii) UPDATE\n");
        printf("\t\tiv) DELETE\n");
        printf("\t\t-> COMMAND FORMAT\n");
        printf("\t\t<OP_CODE>:::<key>:::<value>\n");
        rc = ERROR;
        goto rtn;
    }

  rtn:
    funcExit(logF, ipAddress, "KVClient_CLA_check", rc);
    return rc;

} // End of KVClient_CLA_check()

/*****************************************************************
 * NAME: setUpTCP 
 *
 * DESCRIPTION: This function is designed to create a TCP socke 
 *              and bind to the port 
 *              
 * PARAMETERS: 
 *            (char *) portNo: port number
 *            (char *) ipAddress: IP Address
 * 
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int setUpTCP(char * portNo, char * ipAddress)
{

    funcEntry(logF, ipAddress, "setUpTCP");

    int rc = SUCCESS,
        i_rc;

    tcp = socket(AF_INET, SOCK_STREAM, 0);
    if ( ERROR == tcp )
    {
        printf("\nUnable to open socket\n");
        printf("\nError number: %d\n", errno);
        printf("\nExiting.... ... .. . . .\n");
        perror("socket");
        rc = ERROR;
    }

    printToLog(logF, ipAddress, "socket sucessful");

  rtn:
    funcExit(logF, ipAddress, "setUpTCP", rc);
    return rc;

} // End of setUpTCP()

/*****************************************************************
 * NAME: spawnHelperThreads 
 *
 * DESCRIPTION: This function spawns helper threads 
 *              
 * PARAMETERS: NONE
 * 
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
/*
int spawnHelperThreads()
{

    funcEntry(logF, ipAddress, "spawnHelperThreads");

    int rc = SUCCESS,                    // Return code
        i_rc,                            // Temp RC
        threadNum = 0,                   // Thread counter
        *ptr[NUM_OF_THREADS];            // Pointer to thread counter

    register int counter;                // Counter variable

    pthread_t threadID[NUM_OF_THREADS];  // Helper threads
*/
    /*
     * Create threads:
     */

/*
    for ( counter = 0; counter < NUM_OF_THREADS; counter++ )
    {
        ptr[counter] = (int *) malloc(sizeof(int));
        *(ptr[counter]) = counter;

        i_rc = pthread_create(&threadID[counter], NULL, startKelsa, (void *) ptr[counter]);
        if ( SUCCESS != i_rc )
        {
            printf("\npthread creation failure\n");
            printf("\nError ret code: %d, errno: %d\n", i_rc, errno);
            printf("\nExiting.... ... .. . . .\n");
            rc = ERROR;
            goto rtn;
        }
        sprintf(logMsg, "pthread successful with %d counter", *ptr[counter]);
        printToLog(logF, ipAddress, logMsg);
    }

    for ( counter = 0; counter < NUM_OF_THREADS; counter++ )
    {
        pthread_join(threadID[counter], NULL);
    }

  rtn:
    return rc;
} // End of spawnHelperThreads()
*/

/****************************************************************
 * NAME: startKelsa 
 *
 * DESCRIPTION: This is the pthread function 
 *              
 * PARAMETERS: 
 * (void *) threadNum - thread counter
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
/*
void * startKelsa(void *threadNum)
{

    funcEntry(logF, ipAddress, "startKelsa");

    int rc = SUCCESS,                 // Return code
        i_rc,                         // Temp RC
        *counter;                     // Thread counter

    //counter = (int *) malloc(sizeof(int));
    counter = (int *) threadNum;

    sprintf(logMsg, "Counter: %d", *counter);
    printToLog(logF, ipAddress, logMsg);

    pthread_t tid = pthread_self();   // Thread ID

    switch(*counter)
    {
        case 0:
        // First thread displays KV functionalities
        // and sends request to local server
        printToLog(logF, ipAddress, "In clientSender switch case");
        i_rc = clientSenderFunc();
        break;

        case 1:
        // Second thread receives response from servers
        // and prints them
        printToLog(logF, ipAddress, "In client receiver switch case");
        i_rc = clientReceiveFunc();
        break;

        default:
        printf("\nDefault CASE. An ERROR\n");
        break;
    }

    funcExit(logF, ipAddress, "startKelsa", 0);

} // End of startKelsa()
*/

/****************************************************************
 * NAME: clientReceiveFunc 
 *
 * DESCRIPTION: This is the function that takes care:
 *              i) Receiving response from server
 *              ii) Extracting message to determine result op
 *                  code
 *              iii) Print the result
 *              
 * PARAMETERS: NONE 
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int clientReceiveFunc()
{

    funcEntry(logF, ipAddress, "clientReceiverFunc");

    int rc = SUCCESS,
        numOfBytesRec,
        i_rc,
        new_tcp,
        accept = 0;

    char recMsg[LONG_BUF_SZ];

    struct sockaddr_in serverAddress;

    struct op_code *temp = NULL;

    //listen(tcp, LISTEN_QUEUE_LENGTH);

    // The design is to execute client each time for each operation

        memset(recMsg, '\0', LONG_BUF_SZ);
        memset(&serverAddress, 0, sizeof(struct sockaddr_in));
        numOfBytesRec = 0;
        temp = NULL;

        numOfBytesRec = recvTCP(tcp, recMsg, LONG_BUF_SZ);
        // Check if 0 bytes is received 
        if ( SUCCESS == numOfBytesRec )
        {
            printf("\nNumber of bytes received is ZERO = %d\n", numOfBytesRec);
            rc = ERROR;
            goto rtn; 
        }

        printToLog(logF, "RECEIVED MESSAGE IS", recMsg);

        i_rc = extract_message_op(recMsg, &temp);
        if ( ERROR == i_rc )
        {
            printf("\nUnable to extract received message. Return code of extract_message_op = %d\n", i_rc);
            rc = ERROR;
            goto rtn; 
        }
        if ( (i_rc == INSERT_RESULT) || (i_rc == DELETE_RESULT) || (i_rc == UPDATE_RESULT) )
            temp->opcode = i_rc;

        sprintf(logMsg, "RECEIVED OP CODE : %d", temp->opcode);
        printToLog(logF, "REceived OP CODE", logMsg); 

        switch( temp->opcode )
        { 
            case LOOKUP_RESULT:
                printf("\n");
                printf("\t\t*****RESULT*****\n");
                printf("\t\tSUCCESSFUL LOOKUP\n");
                printf("\t\tKEY: %d - VALUE: %s\n", temp->key, temp->value);
                printf("\t\t****************\n");
            break;

            case INSERT_RESULT:
                printf("\n");
                printf("\t\t*****RESULT*****\n");
                printf("\t\tSUCCESSFUL INSERT\n");
                printf("\t\t****************\n");
            break;
 
            case DELETE_RESULT:
                printf("\n");
                printf("\t\t*****RESULT*****\n");
                printf("\t\tSUCCESSFUL DELETE\n");
                printf("\t\t****************\n");
            break;

            case UPDATE_RESULT:
                printf("\n");
                printf("\t\t*****RESULT*****\n");
                printf("\t\tSUCCESSFUL UPDATE\n");
                printf("\t\t****************\n");
            break;

            case ERROR_RESULT:
                printf("\n");
                printf("\t\t*****RESULT*****\n");
                printf("\t\tERROR DURING OPERATION. TRY AGAIN\n");
                printf("\t\t****************\n");
            break;

            default:
                // We shouldn't be here 
                printf("\nGot something else apart from result and I am not supposed to be getting this. Damn it!!!! Default case in switch\n");
                rc = ERROR;
                goto rtn;
        } // End of switch( temp->opcode )

  rtn:
    funcExit(logF, ipAddress, "clientReceiverFunc", rc);
    return rc;

} // End of clientReceiveFunc()

/****************************************************************
 * NAME: clientSenderFunc 
 *
 * DESCRIPTION: This is the function that takes care:
 *              i) Parse the KV client command 
 *              ii) create message based on choice 
 *              iii) choose peer node to send the request
 *              iv) send the request
 *              
 * PARAMETERS: NONE 
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int clientSenderFunc()
{

    funcEntry(logF, ipAddress, "clientSenderFunc");

    int rc = SUCCESS,
        i_rc;

    // Parse the received KV client command 
    i_rc = parseKVClientCmd();
    if ( ERROR == i_rc ) 
    {
        printf("\nUnable to parse Daisy KV Client Command\n");
        rc = ERROR;
        goto rtn;
    }

    // Create message based on OP CODE
    i_rc = createAndSendOpMsg();
    if ( ERROR == i_rc )
    {
        printf("\nUnable to create and send message based on op code\n");
        rc = ERROR;
        goto rtn;
    }

  rtn:
    funcExit(logF, ipAddress, "clientSendFunc", rc);
    return rc;

} // End of clientSenderFunc()

/****************************************************************
 * NAME: parseKVClientCmd 
 *
 * DESCRIPTION: This function parses the KV client command and from it
 *              tokenizes the OP_CODE, key and value 
 *              -> KV Functionalities / OP_CODE
 *              i) INSERT
 *              ii) LOOKUP
 *              iii) UPDATE
 *              iv) DELETE
 * 
 *              -> COMMAND FORMAT
 *              <OP_CODE>:::<key>:::<value>
 *              
 * PARAMETERS: NONE 
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int parseKVClientCmd()
{

    funcEntry(logF, ipAddress, "parseKVClientCmd");

    int rc = ERROR;

    int opCodeSet = 0,
        keySet = 0,
        valueSet = 0,
        insert = 0,
        lookup = 0,
        update = 0,
        delete = 0,
        invalidOpCode = 0;

    char *token; 

    // OP CODE
    token = strtok(KVclientCmd, ":::");
    if ( token != NULL )
    { 
        strcpy(opCode, token);
        opCodeSet = 1;
        if ( (strcasecmp(opCode, "INSERT") == 0) )
            insert = 1;
        else if ( SUCCESS == strcasecmp(opCode, "LOOKUP") )
            lookup = 1;
        else if ( SUCCESS == strcasecmp(opCode, "UPDATE") )
            update = 1;
        else if ( SUCCESS == strcasecmp(opCode, "DELETE") )
            delete = 1;
        else
        {
            invalidOpCode = 1;
            rc = ERROR;
            goto rtn;
        }
        printf("\nPASSED OP CODE is %s\n", opCode);
    }

    // KEY
    token = strtok(NULL, ":::");
    if ( token != NULL )
    {
         strcpy(key, token);
         keySet = 1;
         printf("\nKEY: %s\n", key);
    }

    // VALUE 
    if (insert || update)
    {
        token = strtok(NULL, ":::");
        if ( token != NULL )
        {
            strcpy(value, token);
            valueSet = 1;
            printf("\nVALUE: %s\n", value);
        }
    }

    if ( (insert && keySet && valueSet) || (lookup && keySet && !valueSet) || (update && keySet && valueSet) || (delete && keySet && !valueSet) )
    {
        rc = SUCCESS;
        goto rtn;
    }
    else 
    {
        rc = ERROR;
        printf("\nINVALID SYNTAX\n");
    }

  rtn:
    if (invalidOpCode)
        printf("\nINVALID OP CODE. TRY AGAIN!!!");
    funcExit(logF, ipAddress, "parseKVClientCmd", rc);
    return rc;
   
} // End of parseKVClientCmd()

/****************************************************************
 * NAME: createAndSendOpMsg 
 *
 * DESCRIPTION: This function creates message based on op code 
 *              containing key value to be sent to the local 
 *              server
 *              
 * PARAMETERS: NONE 
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int createAndSendOpMsg()
{

    funcEntry(logF, ipAddress, "createAndSendOpMsg");

    int rc = SUCCESS,
        i_rc,
        numOfBytesSent,
        opCodeInt;

    struct sockaddr_in serverAddr;

    sprintf(logMsg, "Received Op Code is %s", opCode);

    if ( 0 == strcmp (opCode, "INSERT") )
        opCodeInt = INSERT_KV;
    else if ( 0 == strcmp (opCode, "LOOKUP") )
        opCodeInt = LOOKUP_KV;
    else if ( 0 == strcmp (opCode, "DELETE") )
        opCodeInt = DELETE_KV;
    else if ( 0 == strcmp (opCode, "UPDATE") )
        opCodeInt = UPDATE_KV;
    else 
    {
        printf("\nInvalid OP CODE\n");
        rc = ERROR;
        goto rtn;
    }

    memset(msgToSend, '\0', LONG_BUF_SZ);

    printToLog(logF, ipAddress, "Message before create send");
    printToLog(logF, ipAddress, msgToSend);

    memset(&serverAddr, 0, sizeof(struct sockaddr_in));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(atoi(serverPortNo));
    serverAddr.sin_addr.s_addr = inet_addr(clientIpAddr);
    memset(&(serverAddr.sin_zero), '\0', 8);

    i_rc = connect(tcp, (struct sockaddr *) &serverAddr, sizeof(serverAddr));
    if ( i_rc != SUCCESS )
    {
        strcpy(logMsg, "Cannot connect to server");
        printToLog(logF, ipAddress, logMsg);
        printf("\n%s\n", logMsg);
        rc = ERROR;
        goto rtn;
    }

    switch(opCodeInt)
    {

        case INSERT_KV:

            i_rc = create_message_INSERT(atoi(key), value, msgToSend);
            printToLog(logF, ipAddress, "Message returned by create_message_INSERT");
            printToLog(logF, ipAddress, msgToSend);
            if ( ERROR == i_rc )
            {
                printf("\nUnable to create insert message\n");
                rc = ERROR;
                goto rtn;
            }
            sprintf(logMsg, "client port no before append %s", clientPortNo);
            printToLog(logF, ipAddress, logMsg);
            i_rc = append_port_ip_to_message(clientPortNo, clientIpAddr, msgToSend);
            printToLog(logF, ipAddress, "Message returned by append");
            printToLog(logF, ipAddress, msgToSend);
            if ( ERROR == i_rc )
            {
                printf("\nUnable to create insert message\n");
                rc = ERROR;
                goto rtn;
            }
            sprintf(logMsg, "port no: %d ip address %s", atoi(serverPortNo), clientIpAddr);
            printToLog(logF, ipAddress, logMsg);
            i_rc = append_time_consistency_level(ERROR, consistencyLevel, msgToSend);
            if ( ERROR == i_rc )
            {
                printf("\nUnable to create insert message\n");
                rc = ERROR;
                goto rtn;
            }
            numOfBytesSent = sendTCP(tcp, msgToSend, sizeof(msgToSend));
            if ( SUCCESS == numOfBytesSent )
            {
                printf("\nZERO BYTES SENT\n");
                rc = ERROR;
                goto rtn;
            }
  
        break;

        case LOOKUP_KV:
           
            i_rc = create_message_LOOKUP(atoi(key), msgToSend);
            if ( ERROR == i_rc )
            {
                printf("\nUnable to create lookup message\n");
                rc = ERROR;
                goto rtn;
            }
            i_rc = append_port_ip_to_message(clientPortNo, clientIpAddr, msgToSend);
            if ( ERROR == i_rc )
            {
                printf("\nUnable to create lookup message\n");
                rc = ERROR;
                goto rtn;
            }
            i_rc = append_time_consistency_level(ERROR, consistencyLevel, msgToSend);
            if ( ERROR == i_rc )
            {
                printf("\nUnable to create lookup message\n");
                rc = ERROR;
                goto rtn;
            }
            numOfBytesSent = sendTCP(tcp, msgToSend, sizeof(msgToSend));
            if ( SUCCESS == numOfBytesSent )
            {
                printf("\nZERO BYTES SENT\n");
                rc = ERROR;
                goto rtn;
            }

        break;

        case UPDATE_KV:
   
            i_rc = create_message_UPDATE(atoi(key), value, msgToSend);
            if ( ERROR == i_rc )
            {
                printf("\nUnable to create update message\n");
                rc = ERROR;
                goto rtn;
            }
            i_rc = append_port_ip_to_message(clientPortNo, clientIpAddr, msgToSend);
            if ( ERROR == i_rc )
            {
                printf("\nUnable to create update message\n");
                rc = ERROR;
                goto rtn;
            }
            i_rc = append_time_consistency_level(ERROR, consistencyLevel, msgToSend);
            if ( ERROR == i_rc )
            {
                printf("\nUnable to create update message\n");
                rc = ERROR;
                goto rtn;
            }
            numOfBytesSent = sendTCP(tcp, msgToSend, sizeof(msgToSend));
            if ( SUCCESS == numOfBytesSent )
            {
                printf("\nZERO BYTES SENT\n");
                rc = ERROR;
                goto rtn;
            }
 
        break;

        case DELETE_KV:

            i_rc = create_message_DELETE(atoi(key), msgToSend);
            if ( ERROR == i_rc )
            {
                printf("\nUnable to create delete message\n");
                rc = ERROR;
                goto rtn;
            }
            i_rc = append_port_ip_to_message(clientPortNo, clientIpAddr, msgToSend);
            if ( ERROR == i_rc )
            {
                printf("\nUnable to create delete message\n");
                rc = ERROR;
                goto rtn;
            }
            i_rc = append_time_consistency_level(ERROR, consistencyLevel, msgToSend);
            if ( ERROR == i_rc )
            {
                printf("\nUnable to create delete message\n");
                rc = ERROR;
                goto rtn;
            }
            numOfBytesSent = sendTCP(tcp, msgToSend, sizeof(msgToSend));
            if ( SUCCESS == numOfBytesSent )
            {
                printf("\nZERO BYTES SENT\n");
                rc = ERROR;
                goto rtn;
            }

        break;

        default:

            printf("\nINVALID OP CODE\n");
            rc = ERROR;
            goto rtn;

        break;

    } // End of switch(opCode)

  rtn:
    funcExit(logF, ipAddress, "createAndSendOpMsg", rc);
    return rc;

} // End of createAndSendOpMsg()

/*
 * Main function
 */ 

/*****************************************************************
 * NAME: main 
 *
 * DESCRIPTION: Main function of the client of the key-value store 
 *              
 * PARAMETERS: 
 *            (int) argc - number of command line arguments
 *            (char *) argv - two command line arguments apart 
 *                            from argv[0] namely:
 *                            i) Port No of client
 *                            ii) Ip Address of host
 *                            iii) Port no of server
 * 
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int main(int argc, char *argv[])
{

    int rc = SUCCESS,                      // Return code
        i_rc;                              // Temp RC

    printf("\n");
    printf("\t\t***************************************\n");
    printf("\t\t***************************************\n");
    printf("\t\tWelcome to the Embedded Daisy KV client\n");
    printf("\t\t***************************************\n");
    printf("\t\t***************************************\n");
    printf("\n");

    /*
     * Init log file
     */
    i_rc = logFileCreate(CLIENT_LOG);
    if ( i_rc != SUCCESS )
    {
        printf("\nLog file creation error\n");
        rc = ERROR;
        goto rtn;
    }

    funcEntry(logF, "I am starting", "KVclient::main");

    /*
     * Commannd line arguments check 
     */ 
    i_rc = KVClient_CLA_check(argc, argv);
    if ( i_rc != SUCCESS )
    {
        rc ERROR;
        goto rtn;
    }

    /*
     * Copy ip address, port no and server port no
     */
    memset(clientPortNo, '\0', SMALL_BUF_SZ);
    strcpy(clientPortNo, argv[1]);
    printToLog(logF, ipAddress, "CLIENT PORT NO******************");
    printToLog(logF, ipAddress, argv[1]);
    printToLog(logF, "BUFFER", clientPortNo);
    memset(clientIpAddr, '\0', SMALL_BUF_SZ);
    strcpy(clientIpAddr, argv[2]);
    memset(serverPortNo, '\0', SMALL_BUF_SZ);  
    strcpy(serverPortNo, argv[3]);
    memset(KVclientCmd, '\0', LONG_BUF_SZ);
    strcpy(KVclientCmd, argv[4]);
    consistencyLevel = atoi(argv[5]);
    printToLog(logF, "CONSISTENCY LEVEL CHOSEN", argv[5]);

    strcpy(ipAddress, clientIpAddr);

    /*
     * Set up TCP 
     */
    i_rc = setUpTCP(clientPortNo, clientIpAddr);
    if ( i_rc != SUCCESS )
    {
         rc = ERROR;
         goto rtn;
    }

    /*
     * Parse the received command and send to local server
     */
    i_rc = clientSenderFunc();
    if ( i_rc != SUCCESS )
    {
         rc = ERROR;
         goto rtn;
    }

    i_rc = clientReceiveFunc();
    if ( i_rc != SUCCESS )
    {
         rc = ERROR;
         goto rtn;
    }

  rtn:
    close(tcp);
    return rc;

} // End of main()


