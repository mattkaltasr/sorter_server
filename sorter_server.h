#ifndef __SORTER_SERVER_H__
#define __SORTER_SERVER_H__

#define GET_ID_REQUEST "@RQ_GET_ID@"
#define SORT_REQUEST "@RQ_SORT@"
#define DUMP_REQUEST "@RQ_DUMP@"
#define END_OF_MESSAGE "@MSG_END@"

#define MAX_MESSAGE_LENGTH 1024
#define MAX_REQUEST_TYPE_LENGTH 20
#define SESSION_ID_LENGTH 5
#define MAX_FIELD_LENGTH 3
#define MAX_CLIENTS 10

/* data structure to imb record in csv file */
struct movie_imdb_data {
	char color[15];
	char director_name[100];
	int num_critic_for_reviews;
	int duration; /* in days not date and time */
	int director_facebook_likes;
	int actor_3_facebook_likes;
	char actor_2_name[100];
	int actor_1_facebook_likes;
	int gross;
	char genres[100];
	char actor_1_name[100];
	char movie_title[120];
	int num_voted_users;
	int cast_total_facebook_likes;
	char actor_3_name[100];
	int facenumber_in_poster;
	char plot_keywords[100];
	char movie_imdb_link[120];
	int num_user_for_reviews;
	char language[25];
	char country[25];
	char content_rating[8];
	int budget;
	int title_year;
	int actor_2_facebook_likes;
	double imdb_score;
	double aspect_ratio;
	int movie_facebook_likes;
};

/* enumeration to represent boolean value */
typedef enum {TRUE = 1, FALSE = 0} BOOLEAN;

/* struct to represent required arguments for worker threads */
typedef struct worker_args {
  int sock;
  pthread_t thread;
  pthread_mutex_t start_mutex;
} worker_args_t;

/* dynamic list data sructure of imb records */
typedef struct records_storage {
  struct movie_imdb_data * data;
  int size;
  int capacity;
} records_storage_t;

/* struct to represend cliet information */
typedef struct client {
  int session_id;
  records_storage_t storage;
  BOOLEAN data_received;
  int field;
  pthread_mutex_t access_mutex;
} client_t;


/* function to process client request in separate thread */
void* process_client(void * args);
/* function to get next session id for client, thread safe */
int get_next_session_id();

/* function to parse raw client request and extract request code and arguments */
void parse_request(char request[MAX_MESSAGE_LENGTH + 1], int * session_id, char command[MAX_REQUEST_TYPE_LENGTH + 1],
                     char argument[MAX_MESSAGE_LENGTH], BOOLEAN * end_exists);

/* help function for merge_sort */
void merge(struct movie_imdb_data * arr, int left, int mid, int right, int field);
/* function to sort imb records using merge algorithm */
void merge_sort(struct movie_imdb_data * arr, int left, int right, int field);

/* function to trim leading whitespace */
char* trim (char* string);
/* function to compare imb records */
int compareArrayFields(struct movie_imdb_data field1, struct movie_imdb_data field2, int fieldToBeAcc);

/* function to add imb record to storage */
void add_record(records_storage_t * storage, struct movie_imdb_data item);
/* function to parse imb record from c-string */
BOOLEAN try_get_imdb_record(const char * data, struct movie_imdb_data * imb);

/* functions to process specific client request */
BOOLEAN process_get_session_id_request(int sock);
BOOLEAN prosess_sort_request(int sock, const char * data, const char * column, records_storage_t * storage);
BOOLEAN process_dump_request(int sock, int session_id);

#endif /* __SORTER_SERVER_H__ */
