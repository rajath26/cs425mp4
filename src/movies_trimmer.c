#include <stdio.h>
#include <string.h>
#include <glib.h>
#define WMAX 7 

int main(int argc, char *argv[])
{

     char movies[8096];
     char value[8096];
     char words[WMAX][100] = {"A", "or", "the", "of", "is", "are", "and"};
     register int counter;
     int commonFound;
     int intKey;
     char command[8096];

     strcpy(movies, argv[1]);
     strcpy(value, movies);

     for (counter = 0; counter < WMAX; counter++)
         printf("%s", words[counter]);

     char * token = strtok(movies, " ");
     while ( NULL != token )
     {

          printf("\n%s\n", token);
         
          for ( counter = 0; counter < WMAX; counter++ )
          {

              printf("\n----%s-----\n", words[counter]);
              if (0 == strcmp(words[counter], token))
              {
                  commonFound = 1;
              }
          }

          //if (commonFound)
               //goto strtokLabel;

          if ( !commonFound )
          intKey = g_str_hash(token);

          sprintf(command, "./KVclient 4001 192.17.11.140 3491 \"INSERT:::%d:::%s\" 1", intKey, value);

          //system(command);
          if (!commonFound)
          printf("\n%s\n", command);

          token = strtok(NULL, " ");
          commonFound = 0;

          

     }

     return 0;

} // End of int main()
