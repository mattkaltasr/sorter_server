#ifndef __SORTER_CLIENT_H__
#define __SORTER_CLIENT_H__

#define GET_ID_REQUEST "@RQ_GET_ID@"
#define SORT_REQUEST "@RQ_SORT@"
#define DUMP_REQUEST "@RQ_DUMP@"
#define END_OF_MESSAGE "@MSG_END@"

#define GET_ID_REPLY "@RP_GET_ID@"
#define SESSION_ID_LENGTH 5

#define MAX_MESSAGE_LENGTH 1024
#define MAX_REQUEST_TYPE_LENGTH 20

#define MAX_FILE_NAME 4096

#define HOST_ADDRESS_LENGTH 40

#define EXPECTED_TOKENS 28
#define COLUMN_TO_SORT_NAME_LENGTH 25
#define MAX_FIELD_LENGTH 3

typedef enum {TRUE = 1, FALSE = 0} BOOLEAN;

/* structure to represent entry in socket pool*/
typedef struct socket_pool_entry {
  int sock;
  BOOLEAN free;
} socket_pool_entry_t;

typedef struct worker_args {
  char host[HOST_ADDRESS_LENGTH];
  int port;
  int session_id;
  char * filename;
  int field;
} worker_args_t;

/* function to get string with IP address from string with hostname */
char * host_ip(const char * hostname);
/* function to create socket binded to address and port */
int create_socket(const char * address, int port);
/* function to construct request in appropriate format, acceptabe by server */
char * construct_request(const char * command, int session_id, const char * argument, const char * end_msg);
/* function to send data through socket and log if error occurred */
BOOLEAN send_log_if_error(int sock, const char * request, const char * error_message);
/* function to get reply from server */
BOOLEAN get_reply(int sock, char buffer[MAX_MESSAGE_LENGTH]);
/* function to get session_id from server */
int get_session_id(const char * address, int port);
/* function to get dynamic list of csv file names */
char ** get_csv_files(const char * directory, int * files_num);
/* function that contains main logic of processing single csv file and sending data to server */
void * worker_job(void * args);
/* function to request sorted data from server */
void get_sorted_data(const char * host, int port, int session_id, int field, const char * out_dir);

#endif /*__SORTER_CLIENT_H__*/
