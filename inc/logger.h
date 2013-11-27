//////////////////////////////////////////////////////////////////////////////
//****************************************************************************
//
//    FILE NAME: logger.h 
//
//    DECSRIPTION: This is the header file for logger functionality  
//
//    OPERATING SYSTEM: Linux UNIX only
//    TESTED ON:
//
//    CHANGE ACTIVITY:
//    Date        Who      Description
//    ==========  =======  ===============
//    09-28-2013  Rajath   Initial creation
//
//****************************************************************************
//////////////////////////////////////////////////////////////////////////////

/*
 * Header files
 */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

/*
 * Macros
 */
#define LOG_FILE_LOCATION "machine.log"
#define CLIENT_LOG        "KVclient.log"
#define ERROR             -1
#define SUCCESS           0

/*
 * Global variables
 */
FILE *logF;

/*
 * Function Declarations
 */
int logFileCreate();
int printToLog(
             FILE *fp,            // File pointer 
             char *keyMessage,    // Message to be written as key 
             char *valueMessage   // Message to be written as value 
            ); 
int logFileClose(
                 FILE *fp         // File pointer 
                );
int funcEntry(
              FILE *fp,           // File pointer, 
              char *keyMessage,   // Key
              char *valueMessage  // Value
             );
int funcExit(
             FILE *fp,           // File pointer, 
             char *keyMessage,   // Key
             char *valueMessage, // Value
             int f_rc            // Function RC
             );

/*
 * End 
 */
