#include<pthread.h>

#define UP 1
#define DOWN 0
#define MAX_HOSTS 10 
#define TFAIL 4 
#define JOIN_OPCODE 1
#define LEAVE_OPCODE 2
#define MAX_CHOSEN_HOSTS 2
#define HOSTID_LENGTH 20 
#define IP_ADDRESS_LEN 30 
#define PORT_LENGTH 5
#define TIME_STAMP_LEN 15 
#define HEART_BEAT_UPDATE_SEC 1
#define TREMOVE 20

pthread_mutex_t table_mutex;

int host_no=0;

int reOrderTrigger = 0;
int systemIsLeaving = 0;

struct two_hosts{
  int host_id;
  int valid;
};

struct hb_entry {
  int  valid;
  char host_id[HOSTID_LENGTH];  // combination of host_id and time_of_creation
  char IP[IP_ADDRESS_LEN];
  char port[PORT_LENGTH];
  int hb_count;
  char time_stamp[TIME_STAMP_LEN];
  int status;
  char tcp_port[PORT_LENGTH];
};

char ip_Address[100]="";

struct hb_entry entry[MAX_HOSTS];  // this table is used to extract values from the message
struct hb_entry hb_table[MAX_HOSTS];  // this is the heart beat table mantained for a single host

int initialize_table_with_member(char *, char *, int);
int update_my_heartbeat();
int check_table_for_failed_hosts();
void periodic_heartbeat_update();
int initialize_table(char *,char *,int,char *);
int create_message(char *);
int print_table(struct hb_entry*);
int update_table(struct hb_entry *);
struct hb_entry* extract_message(char *);
void initialize_two_hosts(struct two_hosts *);
int choose_n_hosts(struct two_hosts *, int);
void go_live(char *);
int clear_temp_entry_table(struct hb_entry*);
