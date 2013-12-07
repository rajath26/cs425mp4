/////////////////////////////////////////////////////////////////////////////
//****************************************************************************
//
//    FILE NAME: host.c
//
//    DECSRIPTION: This is the source file for the leader host 
//                 and the member host
//
//    OPERATING SYSTEM: Linux UNIX only
//    TESTED ON:
//
//    CHANGE ACTIVITY:
//    Date        Who      Description
//    ==========  =======  ===============
//    09-29-2013  Rajath   Initial creation
//    11-07-2013  Rajath   Creating Key-Value store 
//    11-26-2013  Rajath   Helenus creation
//
//****************************************************************************
//////////////////////////////////////////////////////////////////////////////

#include "../inc/host.h"
#include "logger.c"
#include "udp.c"
#include "message_table.c"
#include "key_value_store.c"
#include "tcp.c"

/*
 * Function definitions
 */

/*****************************************************************
 * NAME: CLA_checker 
 *
 * DESCRIPTION: This function is designed to check if command 
 *              line arguments is valid 
 *              
 * PARAMETERS: 
 *            (int) argc - number of command line arguments
 *            (char *) argv - two command line arguments apart 
 *                            from argv[0] namely:
 *                            i) port no
 *                            ii) Ip Address of host
 *                            iii) Host Type 
 *                                 "leader" -> Leader Node
 *                                 "member" -> Member Node
 *                            iv) Host ID
 * 
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int CLA_checker(int argc, char *argv[])
{

    funcEntry(logF, ipAddress, "CLA_checker");

    int rc = SUCCESS;        // Return code

    if ( argc != NUM_OF_CL_ARGS )
    {
        printf("\nInvalid usage\n");
        printf("\nUsage information: %s <port_no> <ip_address> <host_type> <host_id>\n", argv[0]);
        rc = ERROR;
        goto rtn;
    }
    
  rtn:
    funcExit(logF, ipAddress, "CLA_checker", rc);
    return rc;

} // End of CLA_checker()

/*****************************************************************
 * NAME: setUpPorts 
 *
 * DESCRIPTION: This function is designed to create a UDP and 
 *              bind to the port and to create another TCP port
 *              and bind to that port 
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
int setUpPorts(char * portNo, char * ipAddress)
{

    funcEntry(logF, ipAddress, "setUpPorts");
    
    int rc = SUCCESS,        // Return code
        i_rc;                // Temp RC
 
    // Create a UDP socket
    udp = socket(AF_INET, SOCK_DGRAM, 0);
    if ( ERROR == udp )
    {
        printf("\nUnable to open socket\n");
        printf("\nError number: %d\n", errno);
        printf("\nExiting.... ... .. . . .\n");
        perror("socket");
        printToLog(logF, ipAddress, "UDP socket() failure");
        rc = ERROR;
        goto rtn;
    }
  
    printToLog(logF, ipAddress, "UDP socket() successful");

    // Create a TCP socket 
    tcp = socket(AF_INET, SOCK_STREAM, 0);
    if ( ERROR == tcp )
    {
        printf("\nUnable to open socket\n");
        printf("\nError number: %d\n", errno);
        printf("\nExiting.... ... .. . . .\n");
        perror("socket");
        printToLog(logF, ipAddress, "TCP socket() failure");
        rc = ERROR;
    }

    printToLog(logF, ipAddress, "TCP socket() successful");

    memset(&hostAddress, 0, sizeof(struct sockaddr_in));
    hostAddress.sin_family = AF_INET;
    hostAddress.sin_port = htons(atoi(portNo)); 
    hostAddress.sin_addr.s_addr = inet_addr(ipAddress);
    memset(&(hostAddress.sin_zero), '\0', 8);

    // Bind the UDP socket
    i_rc = bind(udp, (struct sockaddr *) &hostAddress, sizeof(hostAddress));
    if ( ERROR == i_rc )
    {
        printf("\nUnable to bind UDP socket\n");
        printf("\nError number: %d\n", errno);
        printf("\nExiting.... ... .. . . .\n");
        perror("bind");
        printToLog(logF, ipAddress, "UDP bind() failure");
        rc = ERROR;
        goto rtn;
    }

    printToLog(logF, ipAddress, "UDP bind() successful");

    i_rc = bind(tcp, (struct sockaddr *) &hostAddress, sizeof(hostAddress));
    if ( ERROR == i_rc )
    {
        printf("\nUnable to bind TCP socket\n");
        printf("\nError number: %d\n", errno);
        printf("\nExiting.... ... .. . . .\n");
        perror("bind");
        printToLog(logF, ipAddress, "TCP bind() failure");
        rc = ERROR;
        goto rtn;
    }

    printToLog(logF, ipAddress, "TCP bind() successful");

  rtn:
    funcExit(logF, ipAddress, "setUpPorts", rc);
    return rc;

} // End of setUpPorts()

/*****************************************************************
 * NAME: requestMembershipToLeader 
 *
 * DESCRIPTION: This function is designed to let member host 
 *              request leader node membership to Daisy 
 *              distributed system
 *              
 * PARAMETERS: 
 *            (char *) leaderPort: leader port number
 *            (char *) leaderIp: leader IP Address
 * 
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int requestMembershipToLeader(int leaderPort, char *leaderIp)
{

    funcEntry(logF, ipAddress, "requestMembershipToLeader");

    int rc = SUCCESS,                            // Return code 
        i_rc,                                    // Temp RC
        numOfBytesSent;                          // Num of bytes sent

    char joinMessage[LONG_BUF_SZ],                // Buffer
         joinOperation[SMALL_BUF_SZ] = "JOIN$",  // Join prefix
         tableMessage[LONG_BUF_SZ];              // Table msg

    /*
     * Construct join message
     */
    printToLog(logF, ipAddress, "Message to be sent leader node is:");
    i_rc = create_message(tableMessage);
    sprintf(joinMessage, "%s%s", joinOperation, tableMessage);
    printToLog(logF, ipAddress, joinMessage);
    printToLog(logF, ipAddress, "Sending message to leader node");
    numOfBytesSent = sendUDP(leaderPort, leaderIp, joinMessage);
    sprintf(logMsg, "Num of bytes of join msg sent to leader: %d", numOfBytesSent);
    printToLog(logF, ipAddress, logMsg);
    // If number of bytes sent is 0
    if ( SUCCESS == numOfBytesSent)
    {
        rc = ERROR; 
        goto rtn;
    }
    printToLog(logF, ipAddress, "Join message sent successfully");

  rtn:
    funcExit(logF, ipAddress, "requestMembershipToLeader", rc);
    return rc;

} // End of requestMembershipToLeader()

/*****************************************************************
 * NAME: CLI_UI 
 *
 * DESCRIPTION: This function is designed to display CLI UI for 
 *              member hosts
 *              
 * PARAMETERS: NONE
 * 
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int CLI_UI()
{

    funcEntry(logF, ipAddress, "CLI_UI");

    int rc = SUCCESS,                    // Return code
        i_rc,                            // Temp RC
        leaderPortNo;                    // Leader port no

    char leaderIpAddress[SMALL_BUF_SZ];  // Buffer to hold leader ip
    
    printf("\n");
    printf("\t\t***********************************************************\n");
    printf("\t\t***********************************************************\n");
    printf("\t\tI am a Member host wanting to join Daisy distributed system\n");
    printf("\t\t***********************************************************\n");
    printf("\t\t***********************************************************\n");
    printf("\n\t\tInput the IP address of the Leader node:\n");
    scanf("%s", leaderIpAddress);
    printf("\n\t\tInput the Port No of the Laeder node:\n");
    scanf("%d", &leaderPortNo);
    sprintf(logMsg, "Trying to join %s at %d", leaderIpAddress, leaderPortNo);
    printToLog(logF, ipAddress, logMsg);
    i_rc = requestMembershipToLeader(leaderPortNo, leaderIpAddress);
    if ( i_rc != SUCCESS )
    {
        rc = ERROR;
        goto rtn;
    }
  
  rtn:
    funcExit(logF, ipAddress, "CLI_UI", rc);
    return rc;

} // End of CLI_UI()

/*****************************************************************
 * NAME: askLeaderIfRejoinOrNew 
 *
 * DESCRIPTION: This function is executed for leader to determine 
 *              if this is the first call of leader or if leader
 *              had crashed and wants to rejoin 
 *              
 * PARAMETERS: NONE
 * 
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int askLeaderIfRejoinOrNew()
{

    funcEntry(logF, ipAddress, "askLeaderIfRejoinOrNew");

    int rc = SUCCESS,                // Return code
        choice,                      // Choice
        memNo;                       // Member host no
 
    char memIP[SMALL_BUF_SZ],        // Member IP
         memPort[SMALL_BUF_SZ];      // Member Port
        

    while(1)
    {
        printf("\n");
        printf("\t\t***************************************\n");
        printf("\t\t***************************************\n");
        printf("\t\tWelcome to the Daisy Distributed System\n");
        printf("\t\t***************************************\n");
        printf("\t\t***************************************\n");
        printf("\nIs this:\n 1)First incarnation of the leader or \n2)Reincarnation of the leader to join back?\n");
        scanf("%d", &choice);

        if ( NEW_INCARNATION == choice )
        {
            goto rtn;
        }
        else if ( REINCARNATION == choice )
        {
            printf("\nInput the IP of atleast one other member in the Daisy distributed system:\n");
            scanf("%s", memIP);
            printf("\nInput the Port No of other member (IP chosen above):\n");
            scanf("%s", memPort);
            printf("\nInput host no of the other member (same as one chosen above):\n");
            scanf("%d", &memNo);
            initialize_table_with_member(memPort, memIP, memNo);
            goto rtn;
        }
        else 
        {
            continue;
        }
    } // End of whie(1)

  rtn:
    funcExit(logF, ipAddress, "askLeaderIfRejoinOrNew", rc);
    return rc;

} // End if askLeaderIfRejoinOrNew()

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
int spawnHelperThreads()
{
   
    funcEntry(logF, ipAddress, "spawnHelperThreads");

    int rc = SUCCESS,                    // Return code
        i_rc,                            // Temp RC
        threadNum = 0,                   // Thread counter
        *ptr[NUM_OF_THREADS];         // Pointer to thread counter
    
    register int counter;                // Counter variable
  
    pthread_t threadID[NUM_OF_THREADS];  // Helper threads

    /*
     * Create threads:
     */
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
            printToLog(logF, ipAddress, "pthread() failure");
            goto rtn;
        }
        printToLog(logF, ipAddress, "pthread() success");
        //free(ptr);
    }

    for ( counter = 0; counter < NUM_OF_THREADS; counter++ )
    {
        pthread_join(threadID[counter], NULL);
    }

  rtn:
    funcExit(logF, ipAddress, "spawnHelperThreads", rc);
    return rc;

} // End of spawnHelperThreads();

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
void * startKelsa(void *threadNum)
{

    funcEntry(logF, ipAddress, "startKelsa");

    int rc = SUCCESS,                 // Return code
        i_rc,                         // Temp RC
        *counter;                     // Thread counter

    //counter = (int *) malloc(sizeof(int));
    counter = (int *) threadNum;

    pthread_t tid = pthread_self();   // Thread ID

    sprintf(logMsg, "This is thread with counter: %d and thread ID: %lu", *counter, tid);
    printToLog(logF, ipAddress, logMsg);

    switch(*counter)
    {
        case 0:
        // First thread calls receiver function that does:
        // i) Approve join requests if LEADER
        // ii) Receive heartbeats
        strcat(logMsg, "\texecuting receiverFunc");
        printToLog(logF, ipAddress, logMsg);
        i_rc = receiverFunc(); 
        break;

        case 1:
        // Second thread calls sender function that does:
        // i) Sends heartbeats
        strcat(logMsg, "\texecuting sendFunc");
        printToLog(logF, ipAddress, logMsg);
        i_rc = sendFunc();
        break;

        case 2:
        // Third thread calls heartbeat checker function that:
        // i) checks heartbeat table
        strcat(logMsg, "\texecuting heartBeatCheckerFunc");
        printToLog(logF, ipAddress, logMsg);
        i_rc = heartBeatCheckerFunc();
        break;

	case 3:
	// Fourth thread calls receive key value function that 
	// i) Receives operation instructions from send KV thread
	// ii) Calls respective APIs to perform them on local kV store
	strcat(logMsg, "\texecuting receiveKVFunc");
	i_rc = receiveKVFunc();
	break;

	case 4: 
	// Fifth thread calls key value store reorder function that
	// i) Reorders local key value store whenever a node joins 
	//    the distributed system
	// ii) Reorders local key value store wheneve a node leaves 
	//     the distributed system 
	strcat(logMsg, "\texecuting localKVReorderFunc");
	i_rc = localKVReorderFunc();
	break;

        case 5: 
        // Sixth thread calls functtion to print local key value store
        i_rc = printKVStore();
        break;

        default:
        // Can't get here if we do then exit
        printToLog(logF, ipAddress, "default case. An error");
        goto rtn;
        break;
    } // End of switch

  rtn:
    funcExit(logF, ipAddress, "startKelsa", rc);

} // End of startKelsa()

/****************************************************************
 * NAME: receiverFunc 
 *
 * DESCRIPTION: This is the function that takes care of servicing
 *              the first among the three threads spawned i.e.
 *              the receiver threads which does the following:
 *              i) If I am a LEADER approve join requests from 
 *                 member hosts
 *              ii) Receive heartbeats
 *              
 * PARAMETERS: NONE 
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int receiverFunc()
{

    funcEntry(logF, ipAddress, "receiverFunc");

    int rc = SUCCESS,                    // Return code
        numOfBytesRec,                   // Number of bytes received
        i_rc,                            // Temp RC
        op_code;                         // Operation code

    char recMsg[LONG_BUF_SZ],            // Received message
         tokenRecMsg[LONG_BUF_SZ],       // Received message without join op code 
         buffer[SMALL_BUF_SZ];           // Temp buffer

    struct sockaddr_in memberAddress;    // Address of host 
 
    struct hb_entry * recMsgStruct;      // Heart beat table that holds received message

    recMsgStruct = (struct hb_entry *) malloc(4*sizeof(struct hb_entry));

    /*
     * 1) Receive UDP packet
     * 2) Check operation code
     * 3) If JOIN message: 
     *    i) Extract message
     *    ii) Update heartbeat table
     * 4) Else
     *    i) Extract message 
     *    ii) Update heartbeat table
     */

    for(;;)
    {
        /////////
        // Step 1
        /////////
        memset(recMsg, '\0', LONG_BUF_SZ);
        
        // Debug
        printToLog(logF, "recMsg before recvUDP", recMsg);

        numOfBytesRec = recvUDP(recMsg, LONG_BUF_SZ, memberAddress);
        // Check if 0 bytes is received 
        if ( SUCCESS == numOfBytesRec )
        {
             sprintf(logMsg, "Number of bytes received is ZERO = %d", numOfBytesRec);
             printf("\n%s\n", logMsg);
             printToLog(logF, ipAddress, logMsg);
             continue;
        }

        // Debug
        printToLog(logF, "recMsg after recvUDP", recMsg);

        /////////
        // Step 2
        /////////
        i_rc = checkOperationCode(recMsg, &op_code, tokenRecMsg);
        if ( i_rc != SUCCESS ) 
        {
            printToLog(logF, ipAddress, "Unable to retrieve opcode");
            continue;
        }
        /////////
        // Step 3
        /////////
        if ( JOIN_OP_CODE == op_code )
        {
            sprintf(logMsg, "JOIN msg from %s", inet_ntop(AF_INET, &memberAddress.sin_addr, buffer, sizeof(buffer)));
            printToLog(logF, ipAddress, logMsg);
            ///////////
            // Step 3i
            ///////////
            
            // Debug. uncomment if req
            printToLog(logF, ipAddress, "\nBefore clear_temp_entry_table\n"); 

            clear_temp_entry_table(recMsgStruct);

            printToLog(logF, ipAddress, "\nAfter c_t_e_t\n");

            printToLog(logF, ipAddress, "\nbefore extract_msg\n");

            sprintf(logMsg, "Token received message before e_m: %s", tokenRecMsg);
            printToLog(logF, ipAddress, logMsg);

            recMsgStruct = extract_message(tokenRecMsg);

            printToLog(logF, ipAddress, "\nafter e_m\n");

            //printToLog(logF, ipAddress, "\nToken Received Message: %s", tokenRecMsg);

            sprintf(logMsg, "Token received message: %s", tokenRecMsg);
            printToLog(logF, ipAddress, logMsg);

            if ( NULL == recMsgStruct )
            {
                printToLog(logF, ipAddress, "Unable to extract message");
                continue;
            }
            ////////////
            // Step 3ii
            ////////////
            i_rc = update_table(recMsgStruct);
            if ( i_rc != SUCCESS )
            {
                 printToLog(logF, ipAddress, "Unable to update heart beat table");
                 continue;
            }

        } // End of if ( JOIN_OP_CODE == op_code )
        /////////
        // Step 4
        /////////
        else
        {
            //////////
            // Step 4i
            //////////
            clear_temp_entry_table(recMsgStruct);
            recMsgStruct = extract_message(recMsg);
            if ( NULL == recMsgStruct )
            {
                printToLog(logF, ipAddress, "Unable to extract message");
                continue;
            }
            ///////////
            // Step 4ii
            ///////////
            i_rc = update_table(recMsgStruct);
            if ( i_rc != SUCCESS )
            {
                 printToLog(logF, ipAddress, "Unable to update heart beat table");
                 continue;
            }
        } // End of else
    } // End of for(;;)

  rtn:
    funcExit(logF, ipAddress, "receiverFunc", rc);
    return rc;

} // End of receiverFunc()

/****************************************************************
 * NAME: checkOperationCode 
 *
 * DESCRIPTION: This is the function checks the passed in message
 *              and determines if it is a JOIN message or not
 *              
 * PARAMETERS: 
 * (char *) recMsg - received message
 * (int) op_code - pass by reference back to calling function
 *                 JOIN_OP_CODE if JOIN message else 
 *                 RECEIVE_HB_OP_CODE
 * (char *) tokenRecMsg - pass by reference back to calling 
 *                        function if JOIN_OP_CODE. Message with
 *                        JOIN OP CODE removed
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int checkOperationCode(char *recMsg, int *op_code, char *tokenRecMsg)
{

    funcEntry(logF, ipAddress, "checkOperationCode");
    
    int rc = SUCCESS;              // Return code
 
    char *token;                   // Token

    // Debug
    printToLog(logF, "recMsg in checkOpCode", recMsg); 

    token = strtok(recMsg, "$");

    // Debug
    printToLog(logF, "token *****", token);

    if ( (NULL != token) && (SUCCESS == strcmp(token, "JOIN")) ) 
    {
        printToLog(logF, ipAddress, "JOIN Op");
        *op_code = JOIN_OP_CODE;
        // Debug
        printToLog(logF, "token *****", token); 
        token = strtok(NULL, "$"); 
        // Debug
        printToLog(logF, "token *****", token);
        strcpy(tokenRecMsg, token);
        printToLog(logF, ipAddress, tokenRecMsg);
    }
    else
    {
        *op_code = RECEIVE_HB_OP_CODE;
        printToLog(logF, ipAddress, "RECEIVE HB Op");
        printToLog(logF, ipAddress, recMsg);
    }
    
  rtn:
    funcExit(logF, ipAddress, "checkOperationCode", rc);
    return rc;

} // End of checkOperationCode()

/****************************************************************
 * NAME: sendFunc 
 *
 * DESCRIPTION: This is the function that takes care of sending
 *              heartbeats 
 *              
 * PARAMETERS: NONE 
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int sendFunc()
{

    funcEntry(logF, ipAddress, "sendFunc");

    int rc = SUCCESS,                      // Return code
        num_of_hosts_chosen,               // Number of hosts chosen 
        i_rc,                              // Temp RC
        numOfBytesSent,                    // Number of bytes sent
        portNo;                            // Port no

    register int counter;                  // Counter

    char msgToSend[LONG_BUF_SZ],           // Message to be sent
         ipAddr[SMALL_BUF_SZ],             // IP Address buffer
         portNoChar[SMALL_BUF_SZ];         // Port no

    struct two_hosts hosts[GOSSIP_HOSTS],  // An array of two_hosts
           *ptr;                           // Pointer to above
 
    ptr = hosts;

    while(1)
    {

        memset(msgToSend, '\0', LONG_BUF_SZ);

        // Debug
        printToLog(logF, "SENDOct3", msgToSend);

        initialize_two_hosts(ptr);
        num_of_hosts_chosen = choose_n_hosts(ptr, GOSSIP_HOSTS);

        sprintf(logMsg, "Number of hosts chosen to gossip: %d", num_of_hosts_chosen);
        printToLog(logF, ipAddress, logMsg);

        for ( counter = 0; counter < num_of_hosts_chosen; counter++ )
        {
            printToLog(logF, "PORT NO*****", hb_table[hosts[counter].host_id].port);
            strcpy(portNoChar, hb_table[hosts[counter].host_id].port);
            portNo = atoi(portNoChar);
            strcpy(ipAddr, hb_table[hosts[counter].host_id].IP);
            printToLog(logF, "IP ADDR*****", hb_table[hosts[counter].host_id].IP);
            // create message
            i_rc = create_message(msgToSend);
            if ( SUCCESS != i_rc )
            {
                printToLog(logF, ipAddress, "Unable to create message");
                continue;
            }

            // Debug
            printToLog(logF, "SENDOct3 after create_message", msgToSend);

            // Send UDP packets
            numOfBytesSent = sendUDP(portNo, ipAddr, msgToSend);
            // check if 0 bytes is sent
            if ( SUCCESS == numOfBytesSent )
            {
                printToLog(logF, ipAddress, "ZERO bytes sent");
                continue;
            }
            
            memset(msgToSend, '\0', LONG_BUF_SZ);
        } // End of for ( counter = 0; counter < num_of_hosts_chosen; counter++ )
        sleep(1);
    } // End of while 
    
  rtn:
    funcExit(logF, ipAddress, "sendFunc", rc);
    return rc;

} // End of sendFunc() 

/****************************************************************
 * NAME: heartBeatChecker 
 *
 * DESCRIPTION: This is the function that takes care of checking 
 *              heartbeats
 *              
 * PARAMETERS: NONE 
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int heartBeatCheckerFunc()
{

    funcEntry(logF, ipAddress, "heartBeatCheckerFunc");

    int rc = SUCCESS;        // Return code

    while(1)
    {
        sleep(HEART_BEAT_UPDATE_SEC);
        update_my_heartbeat();
        check_table_for_failed_hosts();
        //print_table(hb_table);
    }

  rtn:
    funcExit(logF, ipAddress, "heartBeatChecker", rc);
    return rc;

} // End of heartBeatChecker()

/****************************************************************
 * NAME: leaveSystem 
 *
 * DESCRIPTION: This is the function that provides 
 *              voluntary leave  
 *              
 * PARAMETERS: NONE 
 *
 * RETURN: VOID
 * 
 ****************************************************************/
void leaveSystem(int signNum)
{

    funcEntry(logF, "Leaving Daisy Distributed System", "leaveSystem");

    int i_rc;

    printToLog(logF, ipAddress, "Preparing the node to leave the Daisy Distributed System");

    // close gossip component first
    //close(udp);

    systemIsLeaving = 1;

    prepareNodeForSystemLeave();
    
    close(udp);
    close(tcp);

    funcExit(logF, "Leaving Daisy Distributed System", "leaveSystem", 0);

} // End of leaveSystem()

/****************************************************************
 * NAME: initialize_local_key_value_store 
 *
 * DESCRIPTION: This is the function that initializes the local
 *              key-value store
 *              
 * PARAMETERS: NONE 
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int initialize_local_key_value_store()
{

    funcEntry(logF, ipAddress, "initialize_key_value_store");

    int rc = SUCCESS,        // Return code
        i_rc;                // Temp RC

    // Create the hash table that stores the local key value store
    i_rc = create_hash_table();
    if ( i_rc != SUCCESS )
    {
         sprintf(logMsg, "\nUnable to create local key value store at IP address %s and host no %d\n", ipAddress, host_no);
	 printf("%s", logMsg);
	 printToLog(logF, ipAddress, logMsg);
	 rc = ERROR;
	 goto rtn;
    }

    printToLog(logF, ipAddress, "Created local key value store successfully");

  rtn:
    funcExit(logF, ipAddress, "initialize_key_value_store", rc);
    return rc;
    
} // End of initialize_key_value_store()

/****************************************************************
 * NAME: receiveKVFunc 
 *
 * DESCRIPTION: This is the function that is responsible for 
 *              i) Receiving request from either the local client
 *                 or a peer server
 *              ii) Extract received and original requestor IP and 
 *                  port and save it
 *              iii) Extract received message 
 *              iv) Determine where to route the request
 *              v) if routing returns local
 *                 then  
 *                     based on op code 
 *                     call respective API
 *                     send result back to the requestor
 *                 else
 *                     send the result to another peer node
 *                 fi
 *              
 * PARAMETERS: NONE 
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int receiveKVFunc()
{

    funcEntry(logF, ipAddress, "receiveKVFunc");

    int rc = SUCCESS,                     // Return code
        i_rc;
    
    socklen_t len;
    
    struct sockaddr_in receivedFromAddr;  // Predecessor address  
 
    pthread_t thread;

    listen(tcp, LISTEN_QUEUE_LENGTH);

    for(;;)
    {

         printToLog(logF, "SEE HERE", "Beginning of server");

         int *ptrSd,
             clientFd;

         ptrSd = (int *) malloc(sizeof(int));

         len = sizeof(receivedFromAddr);

         clientFd = accept(tcp, (struct sockaddr *) &receivedFromAddr, &len);
         if ( clientFd < 0 )
         {
             if ( EINTR == errno )
             {
                 printf("\nINterrpted system call??\n");
                 continue;
             }
         }

         *ptrSd = clientFd;

         sprintf(logMsg, "ACCEPTED SOCKET DESCRIPTOR BY FIRST ACCEPT IN receiveKVFunc() is: %d", clientFd);
         printToLog(logF,"SOCKET DESCRIPTOR", logMsg);

         i_rc = pthread_create(&thread, NULL, FEfunction, (void *)ptrSd);
         if ( i_rc != SUCCESS )
         {
             sprintf(logMsg, "Pthread creation for FE failed");
             printf("\n%s\n", logMsg);
             printToLog(logF, "pthread", logMsg);
             continue;
         }

    }

  rtn:
    funcExit(logF, ipAddress, "receiveKVFunc", rc);
    return rc;

} // End of receiveKVFunc()

/****************************************************************
 * NAME: FEfunction 
 *
 * DESCRIPTION: This is the the thread function of the multi-
 *              threaded front end 
 *              
 * PARAMETERS: 
 *            (void *) sd - Socket Descriptor
 *
 * RETURN:
 * void * 
 * 
 ****************************************************************/
void * FEfunction(void *clientFdPassed)
{

    funcEntry(logF, "THREAD FUNCTION ENTRY", "FEfunction");

    int i_rc,
        resultOpCode = 0,
        insertLocal = 0,
        replicationOpCode = 0,
        numOfBytesRec,
        index, 
        numOfBytesSent,
        peerSocket,
        friendList[NUM_OF_FRIENDS],
        numOfAck, 
        tempAck,
        *ptr = (int *) clientFdPassed,
        clientFd = *ptr;

    register int counter;

    char recMsg[LONG_BUF_SZ] = " ",
         retMsg[LONG_BUF_SZ],
         * lookupValue;

    struct op_code *temp = NULL;          // KV OPCODE

    struct value_group check;
    int iAmAlreadyOwnerOfThis = 0;

         memset(recMsg, '\0', LONG_BUF_SZ);
         memset(retMsg, '\0', LONG_BUF_SZ);
         lookupValue = NULL;
         for ( counter = 0; counter < NUM_OF_FRIENDS; counter++ )
             friendList[counter] = -1;
         numOfAck = 0;
         tempAck = 0;
         resultOpCode = 0;
         insertLocal = 0;
         replicationOpCode = 0;

	 // Debug
	 printToLog(logF, "recMsg before recvTCP", recMsg);

         /////////
	 // Step i
	 /////////
	 // Receive TCP message 
	 numOfBytesRec = recvTCP(clientFd, recMsg, LONG_BUF_SZ);
	 // Check if 0 bytes is received 
	 if ( SUCCESS == numOfBytesRec || ERROR == numOfBytesRec )
	 {
             sprintf(logMsg, "Number of bytes received is ZERO = %d", numOfBytesRec);
	     printf("\n%s\n", logMsg);
	     printToLog(logF, ipAddress, logMsg);
             goto rtn;
	 }

	 // Debug
	 printToLog(logF, "recMsg after recvTCP", recMsg);
         sprintf(logMsg, "number of bytes received %d", numOfBytesRec);
         printToLog(logF, ipAddress, logMsg); 

	 //////////
	 // Step ii
	 //////////
         // Extract and store predecessor and original requestor 
	 // information
         // Original requestor information will be stored in 
	 // temp op_code members port and ipAddr 
	 // predecessor address will be in receivedFromAddr
         //sprintf(logMsg, "Original Requestor Port No: %s, IP Address: %s", temp->port, temp->IP);
         //printToLog(logF, ipAddress, logMsg);

	 ///////////
	 // Step iii
	 ///////////
	 // Extract received message
	 i_rc = extract_message_op(recMsg, &temp);
	 if ( ERROR == i_rc )
	 {
	     sprintf(logMsg, "Unable to extract received message. Return code of extract_message_op = %d", i_rc);
	     printToLog(logF, ipAddress, logMsg);
             goto rtn;
	 }

         printToLog(logF, "successfully extracted message", recMsg);
         sprintf(logMsg, "opcode: %d, key: %d, value: %s", temp->opcode, temp->key, temp->value);
         printToLog(logF, ipAddress, logMsg);

         // If the received message is one of the following op 
         // codes then no need to hash the key
         // INSERT_RESULT
         // LOOKUP_RESULT
         // UPDATE_RESULT
         // DELETE_RESULT
         if ( (INSERT_RESULT == temp->opcode) || (LOOKUP_RESULT == temp->opcode) || (UPDATE_RESULT == temp->opcode) ||  (DELETE_RESULT == temp->opcode) )
             resultOpCode = 1; 
 
         // If the received message is one of the following op 
         // codes then no need to hash they key
         // REP_INSERT
         // REP_DELETE
         // REP_UPDATE
         // REP_LOOKUP
         if ( (REP_INSERT == temp->opcode ) || (REP_DELETE == temp->opcode) || (REP_UPDATE == temp->opcode) || (REP_LOOKUP == temp->opcode) )
             replicationOpCode = 1;

         // If the received message is 
         // INSERT_LEAVE_KV
         // then no need to hash the key. We should just
         // proceed for a local insert
         if ( (INSERT_LEAVE_KV == temp->opcode) )
         {
              insertLocal = 1;
              for ( counter = 0; counter < MAX_HOSTS; counter++ ) 
              {
                  if ( ( 0 == strcmp(hb_table[counter].IP, temp->IP) ) && ( 0 == strcmp(hb_table[counter].port, temp->port) ) )
                  {
                      pthread_mutex_lock(&table_mutex);
                      hb_table[counter].valid = 0;
                      hb_table[counter].status = 0;
                      pthread_mutex_unlock(&table_mutex);
                  } // End of if ( ( 0 == strcmp(hb_table[i].IP, temp.IP) ) && ( 0 == strcmp(hb_table[i].port, temp.port) ) )
              } // End of for ( counter = 0; counter < MAX_HOSTS; counter++ )
         } // End of if ( (INSERT_LEAVE_KV == temp->opcode) )

	 //////////
	 // Step iv
	 //////////
	 // Determine where to route the request
	 // i_rc is the hash index of the host in this case
         // if one of the result op code no need to hash
         if (!resultOpCode || !insertLocal || !replicationOpCode)
         {

             i_rc = update_host_list();
             if ( ERROR == i_rc )
             {
                 sprintf(logMsg, "update host list failed");
                 printToLog(logF, ipAddress, logMsg);
                 goto rtn;
             }
	     index = choose_host_hb_index(temp->key);
	     if ( ERROR == index )
	     {
                 sprintf(logMsg, "Unable to choose host to route the request. Return code of choose_host_hb_index() = %d", i_rc);
	         printToLog(logF, ipAddress, logMsg);
                 goto rtn;
	     }

             sprintf(logMsg, "index : %d", index); 
             printToLog(logF, ipAddress, logMsg);
         }

	 /////////
	 // Step v
	 /////////
	 // If routing returns local index implies we have to perform
	 // the requested operation on our local key value store 
         // If this is a result op code just enter and do nothing
	 // else just send the original message to the host returned 
	 // by choose_host_hb_index

         ///////////
         // If LOCAL 
         ///////////
	 if ( (atoi(hb_table[index].host_id) == my_hash_value) || resultOpCode || insertLocal || replicationOpCode )
	 {

             printToLog(logF, ipAddress, "Local route or result op code or insert_leave from a peer node or replication op code");

             // Do the operation on the local key value store 
	     // based on the KV opcode
	     switch( temp->opcode )
	     {

	         case INSERT_KV:

                     printf("\nINSERT_KV\n");
                     
                     // Choose two friends to replicate this key value
                     i_rc = chooseFriendsForReplication(friendList);
                     if ( ERROR == i_rc )
                     {
                         printToLog(logF, ipAddress, "Unable to choose friends for replication");
                         goto INSERT_ERROR;
                     }

                     // Fill owner and friends' information in temp
                     temp->owner = my_hash_value;
                     temp->friend1 = friendList[0];
                     temp->friend2 = friendList[1];

                     sprintf(logMsg, "FRIENDS CHOSEN: f1: %d: f2: %d", temp->friend1, temp->friend2);
                     printToLog(logF, "FRIENDS CHOSEN", logMsg);
		     
                     // Insert the KV pair in to the KV store
                     i_rc = insert_key_value_into_store(temp);
		     // If error send an error message to the original
		     // requestor
		     if ( ERROR == i_rc )
		     {
                          INSERT_ERROR:
                            // If there was an error during local insert then no need to replicate 
                            // these entries. The FE i.e. the receiveKVFunc thread will send an 
                            // ERROR message back to the KVClient
                            sprintf(logMsg, "There was an ERROR while INSERTING %d = %s KV pair into the local KV store. Since the local insert failed it won't be replicated anywhere else. REPLICATION WONT BE DONE", temp->key, temp->value);
			    printToLog(logF, ipAddress, logMsg);
			    i_rc = create_message_ERROR(retMsg);
			    if ( ERROR == i_rc )
			    {
			        printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                                goto rtn;
			    }
                            i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                            if ( ERROR == i_rc )
                            {
                                printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                                goto rtn;
                            }
                            i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                            if ( ERROR == i_rc )
                            {
                                printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                                goto rtn;
                            }
			    numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
			    if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
			    {
                                printToLog(logF, ipAddress, "ZERO BYTES SENT");
                                goto rtn;
			    }
		     }
                     // If successful replicate and then send a success message 
                     // to the original requestor
		     else 
		     {
                         sprintf(logMsg, "KV pair %d = %s SUCCESSFULLY INSERTED into local key value store", temp->key, temp->value);
			 printToLog(logF, ipAddress, logMsg);
                         sprintf(logMsg, "Attempting replication of the Key Value");
                         printToLog(logF, ipAddress, logMsg);

                         // Increment num of acknowledgements by 1 for 
                         // local successful insert
                         numOfAck++;

                         // Replicate the key value in the friends chosen
                         tempAck = replicateKV(temp, friendList);
                         numOfAck += tempAck;

                         sprintf(logMsg, "NUMBER OF FINAL ACKS %d", numOfAck);
                         printToLog(logF, "NUMBER OF FINAL ACKNOWLEDGEMENTS", logMsg);
                         
                         // The replicateKV function will increment number of acknowledgements
                         // internally based on error or success. Hence check if the number
                         // exceed the set consistency level
                         if ( numOfAck >= temp->cons_level )
                         {
                             i_rc = create_message_INSERT_RESULT_SUCCESS(temp->key, retMsg);
			     if ( ERROR == i_rc )
			     {
			         printToLog(logF, ipAddress, "Error while creating INSERT_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
			     }
                             i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating INSERT_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating INSERT_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
			     numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
	                     if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
		             {
			        printToLog(logF, ipAddress, "ZERO BYTES SENT");
                                goto rtn;
			     }
                         } // End of if ( numOfAck >= temp->cons_level )
                         // If set consistency level number of acknowledgements are not
                         // set then send an error message back to the client
                         else
                             goto INSERT_ERROR;
		     }

                 break;

                 case INSERT_LEAVE_KV:

                     printf("\nINSERT_LEAVE_KV\n");

                     i_rc = chooseFriendsForReplication(friendList);
                     if ( ERROR == i_rc )
                     {
                         printToLog(logF, ipAddress, "Unable to choose friends for replication");
                         goto INSERT_LEAVE_KV_ERROR;
                     }

                     // Fill owner and friends' infromation in temp
                     temp->owner = my_hash_value;
                     temp->friend1 = friendList[0];
                     temp->friend2 = friendList[1];

                     sprintf(logMsg, "INSERT_LEAVE_KV DEBUG INFO owner: %d f1: %d f2: %d", temp->owner, temp->friend1, temp->friend2);
                     printToLog(logF, "INSERT_LEAVE_KV DEBUG INFO", logMsg);
 
                     // Insert the KV pair in to the KV store 
                     i_rc = insert_key_value_into_store(temp);
                     if ( ERROR == i_rc )
                     {
                          INSERT_LEAVE_KV_ERROR:
                            // If there was an error during local insert then no need for replicating
                            // these entries. 
                            sprintf(logMsg, "There was an ERROR while INSERTING %d = %s KV pair into the local KV store", temp->key, temp->value);
                            printToLog(logF, ipAddress, logMsg);
                            i_rc = create_message_ERROR(retMsg);
                            if ( ERROR == i_rc )
                            {
                                printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                                goto rtn;
                            }
                            i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                            if ( ERROR == i_rc )
                            {
                                printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                                goto rtn;
                            }
                            i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                            if ( ERROR == i_rc )
                            {
                                printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                                goto rtn;
                            }
                            numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                            if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                            {
                                printToLog(logF, ipAddress, "ZERO BYTES SENT");
                                goto rtn;
                            }
                     }
                     // If successful replicate and then send a success message 
                     // to the original requestor
                     else
                     {
                         sprintf(logMsg, "KV pair %d = %s SUCCESSFULLY INSERTED into local key value store", temp->key, temp->value);
                         printToLog(logF, ipAddress, logMsg);
                         sprintf(logMsg, "Attempting replication of the Key Value");
                         printToLog(logF, ipAddress, logMsg);

                         // Increment num of acknowledgements by 1 for
                         // local successful insert
                         numOfAck++;

                         // Replicate the key value in the friends chosen
                         tempAck = replicateKV(temp, friendList);
                         numOfAck += tempAck;
           
                         // The replicateKV function will increment number of acknowledgements
                         // internally based on error or success. Hence check if the number
                         // exceed the set consistency level
                         if ( numOfAck >= temp->cons_level )
                         {
                             i_rc = create_message_INSERT_RESULT_SUCCESS(temp->key, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating INSERT_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating INSERT_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating INSERT_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                             if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                             {
                                 printToLog(logF, ipAddress, "ZERO BYTES SENT");
                                 goto rtn;
                             }
                         } // End of if ( numOfAck >= temp->cons_level )
                         else 
                             goto INSERT_LEAVE_KV_ERROR;
                     }

                 break;

		 case DELETE_KV:

                     printf("\nDELETE_KV\n");

                     // Choose two friends to replicate this key value
                     i_rc = chooseFriendsForReplication(friendList);
                     if ( ERROR == i_rc )
                     {
                         printToLog(logF, ipAddress, "Unable to choose friends for replication");
                         goto DELETE_ERROR;
                     }

                      // Fill owner and friends' information in temp
                     temp->owner = my_hash_value;
                     temp->friend1 = friendList[0];
                     temp->friend2 = friendList[1];

                     // Delete the KV pair in to the KV store
                     i_rc = delete_key_value_from_store(temp->key);
                     // If error send an error message to the original 
                     // requestor
		     if ( ERROR == i_rc )
		     {
                         DELETE_ERROR:
                           // If there is an error during local delete then no need to delete the 
                           // replicas
		           sprintf(logMsg, "There was an ERROR while DELETING %d = %s KV pair into the local KV store", temp->key, temp->value);
			   printToLog(logF, ipAddress, logMsg);
                           i_rc = create_message_ERROR(retMsg);
                           if ( ERROR == i_rc )
                           {
                               printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                               goto rtn;
                           }
                           i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                           if ( ERROR == i_rc )
                           {
                               printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                               goto rtn;
                           }
                           i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                           if ( ERROR == i_rc )
                           {
                               printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                               goto rtn;
                           }
                           numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                           if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                           {
                               printToLog(logF, ipAddress, "ZERO BYTES SENT");
                               goto rtn;
                           }
		     }
                     // If successful send a success message to the original 
                     // requestor
		     else
		     {
		         sprintf(logMsg, "KV pair %d = %s SUCCESSFULLY DELETED from local key value store", temp->key, temp->value);
			 printToLog(logF, ipAddress, logMsg);
                         sprintf(logMsg, "Attempting deletion of the replica of the Key Value");
                         printToLog(logF, ipAddress, logMsg);

                         // Increment num of acknowledgements by 1 for
                         // local successful insert
                         numOfAck++;

                         // Replicate the key value in the friends chosen
                         tempAck = replicateKV(temp, friendList);
                         numOfAck += tempAck;
                         
                         // The replicateKV function will increment number of acknowledgements
                         // internally based on error or success. Hence check if the number
                         // exceed the set consistency level
                         if ( numOfAck >= temp->cons_level )
                         {

                             i_rc = create_message_DELETE_RESULT_SUCCESS(temp->key, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating DELETE_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating DELETE_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating DELETE_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                             if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                             {
                                 printToLog(logF, ipAddress, "ZERO BYTES SENT");
                                 goto rtn;
                             }
		         } // End of if ( numOfAck >= temp->cons_level )
                         else
                             goto DELETE_ERROR;
                     } 

		 break;

		 case UPDATE_KV:

                     printf("\nUPDATE_KV\n");

                     // Choose two friends to replicate this key value
                     i_rc = chooseFriendsForReplication(friendList);
                     if ( ERROR == i_rc )
                     {
                         printToLog(logF, ipAddress, "Unable to choose friends for replication");
                         goto UPDATE_ERROR;
                     }

                     // Fill owner and friends' information in temp
                     temp->owner = my_hash_value;
                     temp->friend1 = friendList[0];
                     temp->friend2 = friendList[1];

                     // Update KV in to the KV store
		     i_rc = update_key_value_in_store(temp);
                     // if error send an error message to the original
                     // requestor
		     if ( ERROR == i_rc )
		     {
                         UPDATE_ERROR:
                           // If there is an error during local update then no need to update
                           // the remote replicas
                           sprintf(logMsg, "There was an ERROR while UPDATING %d = %s KV pair into the local KV store", temp->key, temp->value);
			   printToLog(logF, ipAddress, logMsg);
                           i_rc = create_message_ERROR(retMsg);
                           if ( ERROR == i_rc )
                           {
                               printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                               goto rtn;
                           }
                           i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                           if ( ERROR == i_rc )
                           {
                               printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                               goto rtn;
                           }
                           i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                           if ( ERROR == i_rc )
                           {
                               printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                               goto rtn;
                           }
                           numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                           if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                           {
                               printToLog(logF, ipAddress, "ZERO BYTES SENT");
                               goto rtn;
                           }
		     }
		     else 
		     {
                         sprintf(logMsg, "KV pair %d = %s SUCCESSFULLY UPDATED in the local key value store", temp->key, temp->value);
			 printToLog(logF, ipAddress, logMsg);
                         sprintf(logMsg, "Attempting update on replica of the Key Value");
                         printToLog(logF, ipAddress, logMsg);

                         // Increment num of acknowledgements by 1 for
                         // local successful insert
                         numOfAck++;

                         // Replicate the key value in the friends chosen
                         tempAck = replicateKV(temp, friendList);
                         numOfAck += tempAck;
                         
                         // The replicateKV function will increment number of acknowledgements
                         // internally based on error or success. Hence check if the number
                         // exceed the set consistency level
                         if ( numOfAck >= temp->cons_level )
                         {
                             i_rc = create_message_UPDATE_RESULT_SUCCESS(temp->key, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating UPDATE_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating UPDATE_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating UPDATE_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                             if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                             {
                                 printToLog(logF, ipAddress, "ZERO BYTES SENT");
                                 goto rtn;
                             }
		         } // End of if ( numOfAck >= temp->cons_level )
                         else 
                             goto UPDATE_ERROR;
                     }

		 break;

		 case LOOKUP_KV:

                     printf("\nLOOKUP_KV\n");

                     // Choose two friends to replicate this key value
                     i_rc = chooseFriendsForReplication(friendList);
                     if ( ERROR == i_rc )
                     {
                         printToLog(logF, ipAddress, "Unable to choose friends for replication");
                         goto LOOKUP_ERROR;
                     }

                     // Fill owner and friends' information in temp
                     temp->owner = my_hash_value;
                     temp->friend1 = friendList[0];
                     temp->friend2 = friendList[1];

                     // Lookup on the local key value store
	             lookupValue = lookup_store_for_key(temp->key);
                     printToLog(logF, "LOOKUP RETURN STRING", lookupValue);
        
                     // If an error send an error message to the original
                     // requestor
                     if ( NULL == lookupValue ) 
		     {
                         LOOKUP_ERROR:
                         // If the local lookup fails then no need to do remote replica lookup
		         sprintf(logMsg, "There was an ERROR during LOOKUP of %d = %s KV pair in the local KV store", temp->key, temp->value);
			 printToLog(logF, ipAddress, logMsg);
                         i_rc = create_message_ERROR(retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                          if ( ERROR == i_rc )
                          {
                              printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                              goto rtn;
                          }
                         i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                         if ( ERROR == i_rc )
                          {
                              printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                              goto rtn;
                          }
                         numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                         if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                         {
                             printToLog(logF, ipAddress, "ZERO BYTES SENT");
                             goto rtn;
                         }
		     }
                     // If successful send a success message to the orig
                     // requestor
		     else
		     {
		         sprintf(logMsg, "KV pair %d = %s SUCCESSFUL LOOKUP from the local key value store", temp->key, lookupValue);
			 printToLog(logF, ipAddress, logMsg);
                         sprintf(logMsg, "Attempting lookup of replica of the Key Value");
                         printToLog(logF, ipAddress, logMsg);

                         // Increment num of acknowledgements by 1 for
                         // local successful insert
                         numOfAck++;

                         // Replicate the key value in the friends chosen
                         tempAck = replicateKV(temp, friendList);
                         numOfAck += tempAck;
                         
                         // The replicateKV function will increment number of acknowledgements
                         // internally based on error or success. Hence check if the number
                         // exceed the set consistency level
                         if ( numOfAck >= temp->cons_level )
                         {
                             i_rc = create_message_LOOKUP_RESULT(temp->key, lookupValue, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating UPDATE_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating UPDATE_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                             if ( ERROR == i_rc )
                             {
                                 printToLog(logF, ipAddress, "Error while creating UPDATE_RESULT_SUCCESS_MESSAGE");
                                 goto rtn;
                             }
                             numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                             if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                             {
                                 printToLog(logF, ipAddress, "ZERO BYTES SENT");
                                 goto rtn;
                             }
		         } // End of if ( numOfAck >= temp->cons_level )
                         else 
                             goto LOOKUP_ERROR;
                     }

		 break;

                 case REP_INSERT:

                     printf("\nREP_INSERT\n");

                     // Insert the KV pair in to the KV store
                     i_rc = insert_key_value_into_store(temp);
                     // If error send an error message to the original
                     // requestor
                     if ( ERROR == i_rc )
                     {
                         sprintf(logMsg, "There was an ERROR while INSERTING %d = %s KV pair into the local KV store", temp->key, temp->value);
                         printToLog(logF, ipAddress, logMsg);
                         i_rc = create_message_ERROR(retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                         if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                         {
                             printToLog(logF, ipAddress, "ZERO BYTES SENT");
                             goto rtn;
                         }
                     }
                     // If successful send a success message to the original
                     // requestor
                     else
                     {
                         sprintf(logMsg, "KV pair %d = %s SUCCESSFULLY INSERTED", temp->key, temp->value);
                         printToLog(logF, ipAddress, logMsg);
                         i_rc = create_message_INSERT_RESULT_SUCCESS(temp->key, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating INSERT_RESULT_SUCCESS_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating INSERT_RESULT_SUCCESS_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating INSERT_RESULT_SUCCESS_MESSAGE");
                             goto rtn;
                         }
                         numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                         if ( SUCCESS == numOfBytesSent || numOfBytesSent == ERROR )
                         {
                             printToLog(logF, ipAddress, "ZERO BYTES SENT");
                             goto rtn;
                         }
                     }

                 break;
 
                 case REP_DELETE: 

                     printf("\nREP_DELETE\n");

                     check.owner = temp->owner;

                     // Before deleting replica find if I have also become the new owner 
                     // and I am alive in the distributed system
                     if (iAmOwner( (gpointer)(&check), my_hash_value ) )
                         iAmAlreadyOwnerOfThis = 1;

                     if ( iAmAlreadyOwnerOfThis )
                     {
                         i_rc = SUCCESS;
                     }
                     

                     // Delete the KV pair in to the KV store
                     if (!iAmAlreadyOwnerOfThis)
                         i_rc = delete_key_value_from_store(temp->key);
                     // If error send an error message to the original 
                     // requestor
                     if ( ERROR == i_rc )
                     {
                         sprintf(logMsg, "There was an ERROR while DELETING %d = %s KV pair into the local KV store", temp->key, temp->value);
                         printToLog(logF, ipAddress, logMsg);
                         i_rc = create_message_ERROR(retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                         if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                         {
                             printToLog(logF, ipAddress, "ZERO BYTES SENT");
                             goto rtn;
                         }
                     }
                     // If successful send a success message to the original 
                     // requestor
                     else
                     {
                         if (!iAmAlreadyOwnerOfThis)
                         {
                             sprintf(logMsg, "KV pair %d = %s SUCCESSFULLY DELETED", temp->key, temp->value);
                         }
                         else 
                         {
                             strcpy(logMsg, "Deletion skipped because I am already owner");
                         }
                         printToLog(logF, ipAddress, logMsg);
                         i_rc = create_message_DELETE_RESULT_SUCCESS(temp->key, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating DELETE_RESULT_SUCCESS_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating DELETE_RESULT_SUCCESS_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating DELETE_RESULT_SUCCESS_MESSAGE");
                             goto rtn;
                         }
                         numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                         if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                         {
                             printToLog(logF, ipAddress, "ZERO BYTES SENT");
                             goto rtn;
                         }
                     }

                 break;

                 case REP_UPDATE:

                     printf("\nREP_UPDATE\n");

                     // Update KV in to the KV store
                     i_rc = update_key_value_in_store(temp);
                     // if error send an error message to the original
                     // requestor
                     if ( ERROR == i_rc )
                     {
                         sprintf(logMsg, "There was an ERROR while UPDATING %d = %s KV pair into the local KV store", temp->key, temp->value);
                         printToLog(logF, ipAddress, logMsg);
                         i_rc = create_message_ERROR(retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                         if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                         {
                             printToLog(logF, ipAddress, "ZERO BYTES SENT");
                             goto rtn;
                         }
                     }
                     else
                     {
                         sprintf(logMsg, "KV pair %d = %s SUCCESSFULLY UPDATED", temp->key, temp->value);
                         printToLog(logF, ipAddress, logMsg);
                         i_rc = create_message_UPDATE_RESULT_SUCCESS(temp->key, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating UPDATE_RESULT_SUCCESS_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating UPDATE_RESULT_SUCCESS_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating UPDATE_RESULT_SUCCESS_MESSAGE");
                             goto rtn;
                         }
                         numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                         if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                         {
                             printToLog(logF, ipAddress, "ZERO BYTES SENT");
                             goto rtn;
                         }
                     }

                 break;
 
                 case REP_LOOKUP:

                     printf("\nREP_LOOKUP\n");

                     // Lookup on the local key value store
                     lookupValue = lookup_store_for_key(temp->key);
                     printToLog(logF, "LOOKUP RETURN STRING", lookupValue);

                     // If an error send an error message to the original
                     // requestor
                     if ( NULL == lookupValue )
                     {
                         sprintf(logMsg, "There was an ERROR during LOOKUP of %d = %s KV pair in the local KV store", temp->key, temp->value);
                         printToLog(logF, ipAddress, logMsg);
                         i_rc = create_message_ERROR(retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating ERROR_MESSAGE");
                             goto rtn;
                         }
                         numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                         if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                         {
                             printToLog(logF, ipAddress, "ZERO BYTES SENT");
                             goto rtn;
                         }
                     }
                     // If successful send a success message to the orig
                     // requestor
                     else
                     {
                         sprintf(logMsg, "KV pair %d = %s SUCCESSFUL LOOKUP", temp->key, lookupValue);
                         printToLog(logF, ipAddress, logMsg);
                         i_rc = create_message_LOOKUP_RESULT(temp->key, lookupValue, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating UPDATE_RESULT_SUCCESS_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_port_ip_to_message(temp->port, temp->IP, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating UPDATE_RESULT_SUCCESS_MESSAGE");
                             goto rtn;
                         }
                         i_rc = append_time_consistency_level(ERROR, SUCCESS, retMsg);
                         if ( ERROR == i_rc )
                         {
                             printToLog(logF, ipAddress, "Error while creating UPDATE_RESULT_SUCCESS_MESSAGE");
                             goto rtn;
                         }
                         numOfBytesSent = sendTCP(clientFd, retMsg, sizeof(retMsg));
                         if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
                         {
                             printToLog(logF, ipAddress, "ZERO BYTES SENT");
                             goto rtn;
                         }
                     }

                 break;

		 case LOOKUP_RESULT:
                     printf("\nLOOKUP_RESULT\n");
                     // Nothing here as of now 
		 break;

		 case INSERT_RESULT:
                     printf("\nINSERT_RESULT\n");
                     // Nothing here as of now 
		 break;

		 case DELETE_RESULT:
                     printf("\nDELETE_RESULT\n");
                     // Nothing here as of now 
		 break;

		 case UPDATE_RESULT:
                     printf("\nUPDATE_RESULT\n");
                     // Nothing here as of now 
		 break;

                 case ERROR_RESULT:
                     printf("\nERROR_RESULT\n");
                     // Nothing here as of now
                 break;

                 default:
                     printf("\nDEFAULT OP CODE\n");
		     // We should never ever be here 
		     sprintf(logMsg, "Invalid KV OP code received so just continue along");
		     printToLog(logF, ipAddress, logMsg);
                     goto rtn;
		 break;

	     } // End of switch( temp->opcode )

	 } // End of if ( hash_index == my_hash_index )

         //////////////////
         // IF PEER ROUTING
         //////////////////
	 else 
	 {

             // Store the accepted clientFd in a new socket descriptor
             int newClientSd = clientFd;

             sprintf(logMsg, "The new client Socket desc stored before peer routing is %d", newClientSd);
             printToLog(logF, "SOCKET DESCRIPTOR", logMsg);
         
             struct sockaddr_in peerNodeAddr;

             char response[LONG_BUF_SZ];
 
             printToLog(logF, ipAddress, "Peer Node routing");

             int portPN = atoi(hb_table[index].port);
             char ipAddrPN[SMALL_BUF_SZ];
             strcpy(ipAddrPN, hb_table[index].IP);

             sprintf(logMsg, "PEER PORT: %d", portPN);
             printToLog(logF, "PEER PORT", logMsg);
             printToLog(logF, "PEER IP", ipAddrPN);

             // open a new socket
             peerSocket = socket(AF_INET, SOCK_STREAM, 0);
             if ( ERROR == peerSocket )
             {
                 printf("\nUnable to open socket\n");
                 printf("\nError number: %d\n", errno);
                 printf("\nExiting.... ... .. . . .\n");
                 perror("socket");
                 printToLog(logF, ipAddress, "TCP socket() failure");
                 goto rtn;
             }

             sprintf(logMsg, "Socket descriptor opened to peer node is: %d", peerSocket);
             printToLog(logF, "SOCKET DESCRIPTOR", logMsg);

             // connect to the peer node
             memset(&peerNodeAddr, 0, sizeof(struct sockaddr_in));
             peerNodeAddr.sin_family = AF_INET;
             peerNodeAddr.sin_port = htons(portPN);
             peerNodeAddr.sin_addr.s_addr = inet_addr(ipAddrPN);
             memset(&(peerNodeAddr.sin_zero), '\0', 8);

             i_rc = connect(peerSocket, (struct sockaddr *) &peerNodeAddr, sizeof(peerNodeAddr));
             if ( SUCCESS != i_rc )
             {
                 strcpy(logMsg, "Cannot connect to server");
                 printToLog(logF, ipAddress, logMsg);
                 printf("\n%s\n", logMsg);
                 goto rtn;
             }

             // Send the received message to the hashed peer node
             numOfBytesSent = sendTCP(peerSocket, recMsg, LONG_BUF_SZ);
             if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
             {
                 printToLog(logF, ipAddress, "ZERO BYTES SENT");
                 goto rtn;
             }

             // Get the response back from peer node 
             numOfBytesRec = recvTCP(peerSocket, response, LONG_BUF_SZ);
             if ( SUCCESS == numOfBytesRec || numOfBytesRec == ERROR )
             {
                 printToLog(logF, ipAddress, "ZERO BYTES RECEIVED");
                 goto rtn;
             } 
 
             // Send a message back to the local client
             numOfBytesSent = sendTCP(newClientSd, response, LONG_BUF_SZ);
             if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
             {
                 printToLog(logF, ipAddress, "ZERO BYTES SENT");
                 goto rtn;
             }

	 } // End of else of if ( hash_index == my_hash_index )

  rtn:
    printToLog(logF, "SEE HERE", "clientFd closing before the end of the loop");
    close(clientFd);
    printToLog(logF, "SEE HERE", "peerSocket closing before the end of the loop");
    close(peerSocket);
    free(ptr); 
    funcExit(logF, "THREAD FUNCTON EXIT", "FEfunction", 0);

} // End of FEfunction()


/****************************************************************
 * NAME: localKVReoderFunc 
 *
 * DESCRIPTION: This is the function that is responsible for 
 *              i) Reordering the local KV store whenever the
 *                 trigger for reoder is set
 *              ii) The trigger for reorder is set when a node 
 *                  joins or leaves the Daisy Distributed 
 *                  system
 *              
 * PARAMETERS: NONE 
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int localKVReorderFunc()
{

    funcEntry(logF, ipAddress, "localKVReorderFunc");

    int rc = SUCCESS,
        i_rc;

    while(1)
    {

        sleep(REORDER_CHECK_TIME_PERIOD);

        printToLog(logF, ipAddress, "I am in localKVReorderFunc");
        sprintf(logMsg, "Reorder trigger value: %d", reOrderTrigger);
        printToLog(logF, ipAddress, logMsg);

        guint m = g_hash_table_size(key_value_store);

        if (reOrderTrigger && !systemIsLeaving && (int)m) 
        {
            printToLog(logF, "REORGANIZING KV STORE", "REORG");
            reorganize_key_value_store();
            reOrderTrigger = 0;
        }

        // Setting 
        reOrderTrigger = 0;

    } // End of while(1)

  rtn:
    funcExit(logF, ipAddress, "localKVReorderFunc", rc);
    return rc;

} // End of localKVReorderFunc

/****************************************************************
 * NAME: printKVStore 
 *
 * DESCRIPTION: This is the function that is responsible for 
 *              printing local KV store
 *              
 * PARAMETERS: NONE 
 *
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int printKVStore()
{

    funcEntry(logF, ipAddress, "printKVStore");

    int rc = SUCCESS,         // Return code
        i_rc;                 // Temp RC

    char input[2];            // Input

    printf("\n");
    printf("\t\t***************************************\n");
    printf("\t\t***************************************\n");
    printf("\t\tWelcome to the Embedded Daisy KV store \n");
    printf("\t\t***************************************\n");
    printf("\t\t***************************************\n");
    for (;;)
    {
        sleep(5);
        //memset(input, '\0', 2);
        printf("\t\tI am a bot and I print the local KV store and my membership list periodically\n");
        //printf("\t\t1)PRINT KV STORE\n");
        //printf("\t\t2)PRINT MEMBERSHIP LIST\n");
        //printf("\t\t$");
        //scanf("%d", &input);
        //fgets(input, 2, stdin);
        //if ( (0 == strcmp(input, "1") ) )
            iterate_hash_table();
        //else if ( (0 == strcmp(input, "2") ) )
            sleep(5);
            print_table(hb_table);
        //else 
        //{
            //printf("\nInvalid Choice. Try after 15 secs\n");
            //sleep(15);
            //continue;
        //}
    }

  rtn:
    funcExit(logF, ipAddress, "printKVStore", rc);
    return rc;

} // End of printKVStore()

/****************************************************************
 * NAME: replicateKV 
 *
 * DESCRIPTION: This is the function that is responsible for 
 *              replication. This is the replication manager.
 *              
 * PARAMETERS: 
 *            (struct op_code **) op_instance - extracted struct
 *            (int *) friendListPtr - pointer to the friend
 *                                    list
 *
 * RETURN:
 * (int) Number of successful operation on replicas
 * 
 ****************************************************************/
int replicateKV(struct op_code * op_instance, int * friendListPtr)
{

    funcEntry(logF, ipAddress, "replicateKV");

    int numOfAck = 0,                          // Number of successful ack
        replicaSocket,                         // Socket descriptor
        replicaPort,                           // Port no
        i_rc,                                  // Temp RC
        numOfBytesSent,                        // Number of bytes sent
        numOfBytesRec;                         // Number of bytes received 

    register int counter;                      // counter 

    char ipAddrReplica[SMALL_BUF_SZ],          // IP address
         replicationMsgToSend[LONG_BUF_SZ],    // Replication message to send
         response[LONG_BUF_SZ];                // Response

    struct sockaddr_in replicaNode;            // Replica Address

    struct op_code *temp = NULL;

    for ( counter = 0; counter < NUM_OF_FRIENDS; counter++)
    {

         memset(replicationMsgToSend, '\0', LONG_BUF_SZ);
         memset(ipAddrReplica, '\0', SMALL_BUF_SZ);
         memset(response, '\0', LONG_BUF_SZ);
         replicaSocket = 0;

         replicaSocket = socket(AF_INET, SOCK_STREAM, 0);
         if ( ERROR == replicaSocket )
         {
             sprintf(logMsg, "Unable to open socket. Error number: %d", errno);
             printToLog(logF, ipAddress, logMsg);
             continue;
         } // End of if ( ERROR == replicaSocket )

         printToLog(logF, "replicateKV", "SOCKET SUCCESSFUL");

         int index = giveIndexForHash(friendListPtr[counter]);
         if ( ERROR == index )
         {
             printToLog(logF, ipAddress, "Unable to get index of friend");
             close(replicaSocket);
             continue;
         }

         printToLog(logF, "replicateKV", "INDEX GOT SUCCESSFULLY");

         replicaPort = atoi(hb_table[index].port);
         strcpy(ipAddrReplica, hb_table[index].IP);

         sprintf(logMsg, "Port No of friend chosen: %d; IP Address of friend chosen: %s ", replicaPort, ipAddrReplica);
         printToLog(logF, "replicateKV", logMsg);
         
         memset(&replicaNode, 0, sizeof(struct sockaddr_in));
         replicaNode.sin_family = AF_INET;
         replicaNode.sin_port = htons(replicaPort);
         replicaNode.sin_addr.s_addr = inet_addr(ipAddrReplica);
         memset(&(replicaNode.sin_zero), '\0', 8);

         i_rc = connect(replicaSocket, (struct sockaddr *) &replicaNode, sizeof(replicaNode));
         if ( SUCCESS != i_rc )
         {
             strcpy(logMsg, "Cannot connect to server during replication");
             printToLog(logF, ipAddress, logMsg);
             printf("\n%s\n", logMsg); 
             close(replicaSocket);
             continue;
         } 

         printToLog(logF, "replicateKV", "CONNECT SUCCESSFUL");

         sprintf(logMsg, "OP CODE RECEIVED %d", op_instance->opcode);
         printToLog(logF, "replicateKV", logMsg);

         // Based on the op code call the respective replication op code create messages 
         switch ( op_instance->opcode )
         {

             case INSERT_KV: 

                 i_rc = create_message_REP_INSERT(op_instance, replicationMsgToSend);
                 printToLog(logF, "replicateKV", "message returned by create_message_REP_INSERT");
                 printToLog(logF, "replicateKV", replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nUnable to create insert replication message\n");
                     close(replicaSocket);
                     continue;
                 } 
                 i_rc = append_port_ip_to_message(op_instance->port, op_instance->IP, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nError while appending port IP to replication message\n");
                     continue;
                 }
                 printToLog(logF, "replicateKV", replicationMsgToSend);
                 i_rc = append_time_consistency_level(op_instance->timeStamp, SUCCESS, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nUnable to create insert replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 printToLog(logF, "replicateKV", replicationMsgToSend);
             
             break;

             case INSERT_LEAVE_KV:

                 i_rc = create_message_REP_INSERT(op_instance, replicationMsgToSend);
                 printToLog(logF, "replicateKV", "message returned by create_message_REP_INSERT");
                 printToLog(logF, "replicateKV", replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nUnable to create insert replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 i_rc = append_port_ip_to_message(op_instance->port, op_instance->IP, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nError while appending port IP to replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 printToLog(logF, "replicateKV", replicationMsgToSend);
                 i_rc = append_time_consistency_level(op_instance->timeStamp, SUCCESS, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nUnable to create insert replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 printToLog(logF, "replicateKV", replicationMsgToSend);

             break;

             case DELETE_KV:
        
                 i_rc = create_message_REP_DELETE(op_instance, replicationMsgToSend);
                 printToLog(logF, ipAddress, "message returned by create_message_REP_DELETE");
                 printToLog(logF, ipAddress, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nUnable to create delete replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 i_rc = append_port_ip_to_message(op_instance->port, op_instance->IP, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nError while appending port IP to replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 printToLog(logF, "replicateKV", replicationMsgToSend);
                 i_rc = append_time_consistency_level(op_instance->timeStamp, SUCCESS, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nUnable to create delete replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 printToLog(logF, "replicateKV", replicationMsgToSend);

             break;

             case UPDATE_KV: 

                 i_rc = create_message_REP_UPDATE(op_instance, replicationMsgToSend);
                 printToLog(logF, ipAddress, "message returned by create_message_REP_UPDATE");
                 printToLog(logF, ipAddress, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nUnable to create update replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 i_rc = append_port_ip_to_message(op_instance->port, op_instance->IP, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nError while appending port IP to replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 printToLog(logF, "replicateKV", replicationMsgToSend);
                 i_rc = append_time_consistency_level(op_instance->timeStamp, SUCCESS, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nUnable to create update replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 printToLog(logF, "replicateKV", replicationMsgToSend);

             break;

             case LOOKUP_KV:
     
                 i_rc = create_message_REP_LOOKUP(op_instance, replicationMsgToSend);
                 printToLog(logF, ipAddress, "message returned by create_message_REP_LOOKUP");
                 printToLog(logF, ipAddress, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nUnable to create update replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 i_rc = append_port_ip_to_message(op_instance->port, op_instance->IP, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nError while appending port IP to replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 printToLog(logF, "replicateKV", replicationMsgToSend);
                 i_rc = append_time_consistency_level(op_instance->timeStamp, SUCCESS, replicationMsgToSend);
                 if ( ERROR == i_rc )
                 {
                     printf("\nUnable to create update replication message\n");
                     close(replicaSocket);
                     continue;
                 }
                 printToLog(logF, "replicateKV", replicationMsgToSend);

             break;

             default:

                 // We should never ever be here 
                 sprintf(logMsg, "Invalid KV OP code received so just continue along");
                 printToLog(logF, ipAddress, logMsg);
                 close(replicaSocket);
                 continue;

             break;

         } // End of switch ( op_code->opcode )

         printToLog(logF, "replicateKV ; Message just before send", replicationMsgToSend);

         // Send the replication message to the peer node
         numOfBytesSent = sendTCP(replicaSocket, replicationMsgToSend, LONG_BUF_SZ);
         if ( SUCCESS == numOfBytesSent || ERROR == numOfBytesSent )
         {
             printToLog(logF, ipAddress, "ZERO BYTES SENT"); 
             close(replicaSocket);
             continue;
         }

         // Get the response back from the peer node 
         numOfBytesRec = recvTCP(replicaSocket, response, LONG_BUF_SZ);
         if ( SUCCESS == numOfBytesRec || ERROR == numOfBytesRec )
         {
             printToLog(logF, ipAddress, "ZERO BYTES RECEIVED");
             close(replicaSocket);
             continue;
         }

         printToLog(logF, "replicateKV", "Response received for replication message");
         printToLog(logF, "replicateKV", response);

         i_rc = extract_message_op(response, &temp);
         if ( ERROR == i_rc )
         {
             sprintf(logMsg, "Unable to extract received message. Return code of extract_message_op = %d", i_rc);
             printToLog(logF, ipAddress, logMsg); 
             close(replicaSocket);
             continue;
         }

         printToLog(logF, "successfully extracted message", response);
         sprintf(logMsg, "opcode: %d", temp->opcode);
         printToLog(logF, "replicateKV", logMsg);

         switch( temp->opcode )
         {

             case INSERT_RESULT:
                 sprintf(logMsg, "REPLICATION OPERATION ON PEER NODE SUCCESSFUL. PEER NODE PORT: %d PEER IP ADDRESS: %s", replicaPort, ipAddrReplica);
                 printf("\n%s\n", logMsg);
                 printToLog(logF, ipAddress, logMsg);
                 numOfAck++;
             break;
 
             case DELETE_RESULT:
                 sprintf(logMsg, "REPLICATION OPERATION ON PEER NODE SUCCESSFUL. PEER NODE PORT: %d PEER IP ADDRESS: %s", replicaPort, ipAddrReplica);
                 printf("\n%s\n", logMsg);
                 printToLog(logF, ipAddress, logMsg);
                 numOfAck++;
             break;

             case UPDATE_RESULT:
                 sprintf(logMsg, "REPLICATION OPERATION ON PEER NODE SUCCESSFUL. PEER NODE PORT: %d PEER IP ADDRESS: %s", replicaPort, ipAddrReplica);
                 printf("\n%s\n", logMsg);
                 printToLog(logF, ipAddress, logMsg);
                 numOfAck++;
             break;

             case LOOKUP_RESULT:
                 sprintf(logMsg, "REPLICATION OPERATION ON PEER NODE SUCCESSFUL. PEER NODE PORT: %d PEER IP ADDRESS: %s", replicaPort, ipAddrReplica);
                 printf("\n%s\n", logMsg);
                 printToLog(logF, ipAddress, logMsg);
                 numOfAck++;
             break;

             case ERROR_RESULT:
                 sprintf(logMsg, "REPLICATION OPERATION ON PEER NODE FAILED. PEER NODE PORT: %d PEER IP ADDRESS: %s", replicaPort, ipAddrReplica);
                 printf("\n%s\n", logMsg);
                 printToLog(logF, ipAddress, logMsg);
             break;

             default:
                 // We should never ever be here 
                 sprintf(logMsg, "Invalid KV OP code received so just continue along");
                 printToLog(logF, ipAddress, logMsg);
                 continue;
             break;

         } // End of switch( temp->opcode )

         close(replicaSocket);
         
    } // End of for ( counter = 0; counter < NUM_OF_FRIENDS; counter++)

  rtn:
    sprintf(logMsg, "Number of successful replication operations: %d", numOfAck);
    printToLog(logF, "NUMBER OF SUCCESSFUL REPLICATION OPERATIONS", logMsg);
    funcExit(logF, ipAddress, "replicateKV", numOfAck);
    return numOfAck;

} // End of replicateKV()


/*
 * Main function
 */

/*****************************************************************
 * NAME: main 
 *
 * DESCRIPTION: Main function of the leader host i.e. the contact 
 *              host that approves other hosts to join the
 *              network and the member host. This binary is 
 *              invoked via a start up script. The parameters are
 *              port no and ip address and node type.
 *              
 * PARAMETERS: 
 *            (int) argc - number of command line arguments
 *            (char *) argv - two command line arguments apart 
 *                            from argv[0] namely:
 *                            i) Port No
 *                            ii) Ip Address of host
 *                            iii) Host Type 
 *                                 "leader" -> Leader Node
 *                                 "member" -> Member Node
 *                            iv) Host ID
 * 
 * RETURN:
 * (int) ZERO if success
 *       ERROR otherwise
 * 
 ****************************************************************/
int main(int argc, char *argv[])
{

    int rc = SUCCESS,              // Return code
        i_rc;                            // Intermittent return code
        
    char leaderIpAddress[SMALL_BUF_SZ],  // Buffer to hold leader ip
         leaderPortNo[SMALL_BUF_SZ];     // Buffer to hold leader port no

    /*
     * Init log file 
     */
    i_rc = logFileCreate(LOG_FILE_LOCATION);
    if ( i_rc != SUCCESS )
    {
        printf("\nLog file won't be created. There was an error\n");
        rc = ERROR;
        goto rtn;
    }

    funcEntry(logF, "I am starting", "host::main");

    /*
     * Command line arguments check
     */
    i_rc = CLA_checker(argc, argv);
    if ( i_rc != SUCCESS )
    {
        rc = ERROR;
        goto rtn;
    }
       
    /*
     * Copy ip address and port no to local buffer
     */
    memset(ipAddress, '\0', SMALL_BUF_SZ);
    sprintf(ipAddress, "%s", argv[2]);
    memset(portNo, '\0', SMALL_BUF_SZ);
    sprintf(portNo, "%s", argv[1]);
    host_no = atoi(argv[4]);

    /*
     * Init local host heart beat table
     */
    initialize_table(portNo, ipAddress, host_no, "1234");
    printToLog(logF, ipAddress, "Initialized my table");

    /*
     * Init the local Key Value store i.e. 
     * the hash table 
     */
    i_rc = initialize_local_key_value_store();
    if ( i_rc != SUCCESS )
    {
         sprintf(logMsg, "Unable to initialize local key value store at IP Address %s and host no %d", ipAddress, host_no);
         printf("\n%s\n", logMsg);
         printToLog(logF, ipAddress, logMsg);
         rc = ERROR;
         goto rtn;
    }
         
    /* 
     * Get the node type based on third argument. By default it
     * is always member node.
     */
    if ( SUCCESS == strcmp(argv[3], LEADER_STRING) )
    {
        isLeader = true;
        printToLog(logF, ipAddress, "I am the leader node");
    }
    else 
    {
        printToLog(logF, ipAddress, "I am a member node");
    }

    /* 
     * Set up UDP & TCP
     */
    i_rc = setUpPorts(portNo, ipAddress); 
    if ( i_rc != SUCCESS )
    {
        rc = ERROR;
        printToLog(logF, ipAddress, "UDP and TCP setup failure");
        goto rtn;
    }

    // Log current status 
    printToLog(logF, ipAddress, "UDP and TCP setup successfully");

    /*
     * If current host is a LEADER then log that this host has
     * joined the distributed system
     */
    if ( isLeader )
    {
        printToLog(logF, ipAddress, "I, THE LEADER have joined the Daisy Distributed System");
    }

    /*
     * Display the CLI UI if this host is a MEMBER host which 
     * asks member if he wants to send join message to leader node
     * and calls the function requestMembershipToLeader() which
     * does the job
     */
    if ( !isLeader )
    {
        i_rc = CLI_UI();
        if ( i_rc != SUCCESS )
        {
            rc = ERROR;
            goto rtn;
        }
    }
    /* 
     * If leader ask if this a new incarnation or a 
     * reincarnation
     */
    else
    {
        i_rc = askLeaderIfRejoinOrNew();
        if ( i_rc != SUCCESS )
        {
            rc = ERROR;
            goto rtn;
        }
    }

    /* 
     * Set up infrastructure for node to leave
     * voluntarily
     */
    signal(SIGABRT, leaveSystem);
    if ( errno != SUCCESS )
    {
        printf("SIGINT set error %d \n", errno);
    }

    /*
     * Spawn the helper threads
     */
    i_rc = spawnHelperThreads();
    if ( i_rc != SUCCESS )
    {
        rc = ERROR;
        goto rtn;
    }


  rtn:
    funcExit(logF, ipAddress, "Host::main", rc);

    /*
     * Close the log
     */ 
    if ( logF != NULL )
    {
        logFileClose(logF);
    }

    /*
     * Close the sockets
     */
    close(tcp);
    close(udp);

    return rc;

} // End of main
