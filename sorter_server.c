//Project 3 CS214
// Matthew Kalita & //Josh Raslowsky 

#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <pthread.h>

#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/types.h>
#include <sys/socket.h>

#include "sorter_server.h"

client_t clients[MAX_CLIENTS];
pthread_mutex_t get_session_mutex;


int main(int argc, char ** argv) {
    int listenfd = 0, connfd = 0, i, port;
    struct sockaddr_in serv_addr;
    struct sockaddr_in client_address;
    worker_args_t * worker_args;
   char clntName[INET_ADDRSTRLEN];
    socklen_t slen;
    time_t rawtime;
    struct tm * timeinfo;

    if (argc != 3 || strcmp(argv[1], "-p") != 0) {
      printf("Wrong comand line arguments. Expected <-p> <port_value>\n");
      return 0;
    }
  
    port = atoi(argv[2]);
  
    for (i = 0; i < MAX_CLIENTS; ++i) {
      clients[i].session_id = -1;  
      clients[i].storage.data = NULL;
      clients[i].storage.size = 0;
      clients[i].storage.capacity= 0;
      clients[i].field = 0;
      clients[i].data_received = FALSE;
    }

    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd == -1) {
        printf("Error! Cannot create socket\n");
        return 0;
    }
    memset(&serv_addr, '0', sizeof(serv_addr));

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(port); 
    slen = sizeof(client_address);

    bind(listenfd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)); 

    listen(listenfd, 100);

    while(1) {
      printf("Listening ...\n");
      connfd = accept(listenfd, (struct sockaddr*)&client_address, &slen);
      
      if(inet_ntop(AF_INET,&client_address.sin_addr.s_addr, clntName, sizeof(clntName))!=NULL){
          time ( &rawtime );
          timeinfo = localtime ( &rawtime );
          printf("Client connection accepted %s - > %s\n", clntName, asctime (timeinfo));
      } else {
        printf("Error! Unable to get client address\n");
     }

      worker_args = malloc(sizeof(worker_args_t));
      if (!worker_args) {
        printf("Error! Cannot allocate memory for worker args\n");
        close(connfd);
      } else {
        pthread_mutex_lock(&worker_args->start_mutex);
      	worker_args->sock = connfd;
      	pthread_create(&worker_args->thread, NULL, process_client, worker_args);
        pthread_detach(worker_args->thread);
        pthread_mutex_unlock(&worker_args->start_mutex);
      }
    }

  return 0;
}

void add_record(records_storage_t * storage, struct movie_imdb_data item) {
  int next_capacity;
  if (storage->size + 1 >= storage->capacity) {
    next_capacity = storage->capacity + 20;
    storage->data = realloc(storage->data, sizeof(struct movie_imdb_data) * next_capacity);
    if(!storage->data) {
      storage->size = 0;
      storage->capacity = 0;
      return;
    }
    storage->capacity = next_capacity;
  }
  storage->data[storage->size] = item;
  storage->size += 1;
}

BOOLEAN try_get_imdb_record(const char * data, struct movie_imdb_data * imb) {
  char * buffer;
  char * token;
  int n = 0;
  
  buffer = malloc(sizeof(char) * (strlen(data) + 1));
  if (!buffer) return FALSE;
  strcpy(buffer, data);

  token = strtok(buffer, ",");
  while(token) {
    switch(++n) {
      case 1:
      strcpy(imb->color, token);
      break;
      case 2:
      strcpy(imb->director_name, token);
      break;
      case 3:
      imb->num_critic_for_reviews = atoi(token);
      break;
      case 4:
      imb->duration = atoi(token);
      break;
      case 5:
      imb->director_facebook_likes = atoi(token);
      break;
      case 6:
      imb->actor_3_facebook_likes = atoi(token);
      break;
      case 7:
      strcpy(imb->actor_2_name, token);
      break;
      case 8:
      imb->actor_1_facebook_likes = atoi(token);
      break;
      case 9:
      imb->gross = atoi(token);
      break;
      case 10:
      strcpy(imb->genres, token);
      break;
      case 11:
      strcpy(imb->actor_1_name, token);
      break;
      case 12:
      strcpy(imb->movie_title, token);
      break;
      case 13:
      imb->num_voted_users = atoi(token);
      break;
      case 14:
      imb->cast_total_facebook_likes = atoi(token);
      break;
      case 15:
      strcpy(imb->actor_3_name, token);
      break;
      case 16:
      imb->facenumber_in_poster = atoi(token);
      break;
      case 17:
      strcpy(imb->plot_keywords, token);
      break;
      case 18:
      strcpy(imb->movie_imdb_link, token);
      break;
      case 19:
      imb->num_user_for_reviews = atoi(token);
      break;
      case 20:
      strcpy(imb->language, token);
      break;
      case 21:
      strcpy(imb->country, token);
      break;
      case 22:
      strcpy(imb->content_rating, token);
      break;
      case 23:
      imb->budget = atoi(token);
      break;
      case 24:
      imb->title_year = atoi(token);
      break;
      case 25:
      imb->actor_2_facebook_likes = atoi(token);
      break;
      case 26:
      imb->imdb_score = atof(token);
      break;
      case 27:
      imb->aspect_ratio = atof(token);
      break;
      case 28:
      imb->movie_facebook_likes = atoi(token);
      break;
      /*default: printf("Unknown imb record field\n"); break;*/
    }
    token = strtok(NULL, ",");
  }

  free(buffer);
  return n == 28;
}

void parse_request(char request[MAX_MESSAGE_LENGTH + 1], int * session_id, char command[MAX_REQUEST_TYPE_LENGTH + 1],
                     char argument[MAX_MESSAGE_LENGTH], BOOLEAN * end_exists) {
  int i, j;
  int request_length = strlen(request);
  char session_str[SESSION_ID_LENGTH + 1];

/*
  printf("===============================================\n");
  printf("%s\n", request);
  printf("===============================================\n");
*/
  /* copy command name till space */
  for (i = 0; i < request_length && i < MAX_REQUEST_TYPE_LENGTH && request[i] != ' '; ++i) {
    command[i] = request[i];
  }
  /* add null terminator */
  command[i] = '\0';

  j= 0;
  for (i = MAX_REQUEST_TYPE_LENGTH; i < request_length 
       && i < MAX_REQUEST_TYPE_LENGTH + SESSION_ID_LENGTH && request[i] != ' '; ++i) {
    session_str[j++] = request[i];
  }
  session_str[j] = '\0';
  *session_id = atoi(session_str);
  /* check if command contains end_command token */
  *end_exists = strstr(request, END_OF_MESSAGE) == NULL ? 0 : 1;
  /* cut end token from request */
  request_length -= (*end_exists ? strlen(END_OF_MESSAGE) : 0);
  j = 0;
  for (i = MAX_REQUEST_TYPE_LENGTH + SESSION_ID_LENGTH; i < request_length; ++i) {
    argument[j++] = request[i];
  }
  /* add null terminator */
  argument[j] = '\0';
}

int get_next_session_id() {
  static int session_counter = -1;
  int result;
  pthread_mutex_lock(&get_session_mutex);
  if (++session_counter > MAX_CLIENTS) {
     session_counter = 0;
  }
  result = session_counter;
  pthread_mutex_unlock(&get_session_mutex);
  return result;
}

BOOLEAN process_get_session_id_request(int sock) {
  int n, session;
  char reply[MAX_MESSAGE_LENGTH];

  session = get_next_session_id();
  n = sprintf(reply, "%*d%s", -SESSION_ID_LENGTH, session, END_OF_MESSAGE);
  return send(sock, reply, n, 0) != -1;
}

BOOLEAN prosess_sort_request(int sock, const char * data, const char * column, records_storage_t * storage) {
  int n;
  char reply[MAX_MESSAGE_LENGTH];
  struct movie_imdb_data record;
  n = sprintf(reply, "OK%s", END_OF_MESSAGE);
  
  if (try_get_imdb_record(data, &record)) {
    add_record(storage, record);
  } else {
    /*printf("Invalid imb record detected\n");*/
  }

  return send(sock, reply, n, 0) != -1;
}

BOOLEAN process_dump_request(int sock, int session_id) {
  char buffer[MAX_MESSAGE_LENGTH + 1];
  int n, i;
  struct movie_imdb_data * d;
  BOOLEAN result = TRUE;

  pthread_mutex_lock(&clients[session_id].access_mutex);
  if (clients[session_id].data_received) {
    n = sprintf(buffer, "No data available for session %d\n", session_id);
    if (send(sock, buffer, n, 0) <= 0) {
       result = FALSE;
    }
  } else {
    merge_sort(clients[session_id].storage.data, 0, clients[session_id].storage.size - 1, clients[session_id].field);
    d = clients[session_id].storage.data;
    for (i = 0; i < clients[session_id].storage.size; ++i) {
      
      n = sprintf(buffer, "%s,%s,%d,%d,%d,%d,%s,%d,%d,%s,%s,%s,%d,%d,%s,%d,%s,%s,%d,%s,%s,%s,%d,%d,%d,%f,%f,%d\n", 
             d[i].color,
             d[i].director_name,
             d[i].num_critic_for_reviews,
             d[i].duration,
             d[i].director_facebook_likes,
             d[i].actor_3_facebook_likes,
             d[i].actor_2_name,
             d[i].actor_1_facebook_likes,
             d[i].gross,
             d[i].genres,
             d[i].actor_1_name,
             d[i].movie_title,
             d[i].num_voted_users,
             d[i].cast_total_facebook_likes,
             d[i].actor_3_name,
             d[i].facenumber_in_poster,
             d[i].plot_keywords,
             d[i].movie_imdb_link,
             d[i].num_user_for_reviews,
             d[i].language,
             d[i].country,
             d[i].content_rating,
             d[i].budget,
             d[i].title_year,
             d[i].actor_2_facebook_likes,
             d[i].imdb_score,
             d[i].aspect_ratio,
             d[i].movie_facebook_likes);
       
       if (send(sock, buffer, n, 0) <= 0) {
         result = FALSE;
         break;
       }

       if (read(sock, buffer, MAX_MESSAGE_LENGTH) <= 0) {
         printf("Error! No reply fro client after partition of data\n");
         break;
       }
    }
  }
  free(clients[session_id].storage.data);
  clients[session_id].storage.size = 0;
  clients[session_id].storage.capacity = 0;
  clients[session_id].data_received = TRUE;
  clients[session_id].storage.data = NULL;
  pthread_mutex_unlock(&clients[session_id].access_mutex);
  
  return result;
}

void * process_client(void * args) {
  char buffer[MAX_MESSAGE_LENGTH + 1];
  char argument[MAX_MESSAGE_LENGTH];
  char command[MAX_REQUEST_TYPE_LENGTH + 1];
  char field_buffer[MAX_FIELD_LENGTH + 1] = "";
  records_storage_t storage;
  
  BOOLEAN end_exists = 0, reply_success = 0;
  int n;
  int sock, session_id, field = 1;


  worker_args_t * wargs = (worker_args_t *)args;
  if (!wargs) {
    printf("Error! Cannot get worker arguments\n");
    return NULL;
  }

  storage.size = 0;
  storage.capacity = 0;
  storage.data = NULL;

  /* wait until main thread allow to process client */
  pthread_mutex_lock(&wargs->start_mutex);
  sock = wargs->sock;

  while (TRUE) {
     buffer[0] = '\0';
     command[0] = '\0';
     argument[0] = '\0';

     n = read(sock, buffer, MAX_MESSAGE_LENGTH);
      
     if (n == -1) {
       printf("Error! Cannot read from client\n");
       break;
     } else if (n == 0) {
       printf("Connection closed by client\n");
       break;
     }

     parse_request(buffer, &session_id, command, argument, &end_exists);
     /*printf("Request is: '%s' %d '%s' %d\n", command, session_id, argument, end_exists);*/
     if (strcmp(command, GET_ID_REQUEST) == 0) {
       reply_success = process_get_session_id_request(sock);
     } else if (strcmp(command, SORT_REQUEST) == 0) {
       if(session_id < 0 || session_id >= MAX_CLIENTS) {
         printf("Invalid session id\n");
         break;
       }
       /* get field number to sort from request argument */
       field_buffer[0] = '\0';
       strncpy(field_buffer, argument, MAX_FIELD_LENGTH);
       field_buffer[MAX_FIELD_LENGTH] = '\0';
       field = atoi(field_buffer);

       if (end_exists) {
         reply_success = TRUE;
         
         /* add all records to client storage from local storge */
         pthread_mutex_lock(&clients[session_id].access_mutex);
         clients[session_id].field = field;
         for (n = 0; n < storage.size; ++n) {
           add_record(&clients[session_id].storage, storage.data[n]);
         }
         pthread_mutex_unlock(&clients[session_id].access_mutex);
       } else {
         reply_success = prosess_sort_request(sock, argument + MAX_FIELD_LENGTH, "", &storage);
       }
     } else if (strcmp(command, DUMP_REQUEST) == 0) {
       reply_success = process_dump_request(sock, session_id);
       break;
     } else {
       printf("Unknown request '%s'\n", buffer);
       break;
     }
    
    if (!reply_success) {
      printf("Failed to process '%s'\n", buffer);
      break;
    }

    if (strstr(buffer, END_OF_MESSAGE)) break;
  }

  pthread_mutex_unlock(&wargs->start_mutex);
  close(sock);
  free(wargs);

  return NULL;
}

char* trim (char* string) {
  char* temp;
  int i = 0;
  while (string[i] == ' ') {
    i++;
  }
  temp = string + i;
  return temp;
}

int compareArrayFields(struct movie_imdb_data field1, struct movie_imdb_data field2, int fieldToBeAcc) {
  if (fieldToBeAcc == 1) {
    return strcmp(trim(field1.color), trim(field2.color));
  } else if (fieldToBeAcc == 2) {
    return strcmp(trim(field1.director_name), trim(field2.director_name));	
  } else if (fieldToBeAcc == 3) {
    if (field1.num_critic_for_reviews < field2.num_critic_for_reviews) {
      return -1;
    } else {
      return 1;
    }	
 } else if (fieldToBeAcc == 4)
	{
		if(field1.duration < field2.duration)
		{
			return -1;
		}
		else
		{
			return 1;
		}
		
	}
	else if (fieldToBeAcc == 5)
	{
		if(field1.director_facebook_likes < field2.director_facebook_likes)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
	else if (fieldToBeAcc == 6)
	{
		if(field1.actor_3_facebook_likes < field2.actor_3_facebook_likes)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
	else if (fieldToBeAcc == 7)
	{
		return strcmp(trim(field1.actor_2_name), trim(field2.actor_2_name));
	}
	else if (fieldToBeAcc == 8)
	{
		if (field1.actor_1_facebook_likes < field2.actor_1_facebook_likes)
		{
			return -1;
		}
		else
		{
			return 1;
		}
		
	}
	else if (fieldToBeAcc == 9)
	{
		if(field1.gross < field2.gross)
		{
			return -1;
		}
		else
		{
			return 1;
		}
		
	}
	else if (fieldToBeAcc == 10)
	{
		return strcmp(trim(field1.genres), trim(field2.genres));
		
	}
	else if (fieldToBeAcc == 11)
	{
		return strcmp(trim(field1.actor_1_name), trim(field2.actor_1_name));
		
	}
	else if (fieldToBeAcc == 12)
	{
		return strcmp(trim(field1.movie_title), trim(field2.movie_title));
	}
	else if (fieldToBeAcc == 13)
	{
		if(field1.num_voted_users < field2.num_voted_users)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
	else if (fieldToBeAcc == 14)
	{
		if(field1.cast_total_facebook_likes < field2.cast_total_facebook_likes)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
	else if (fieldToBeAcc == 15)
	{
		return strcmp(trim(field1.actor_3_name), trim(field2.actor_3_name));
	}
	else if (fieldToBeAcc == 16)
	{
		if(field1.facenumber_in_poster < field2.facenumber_in_poster)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
	else if (fieldToBeAcc == 17)
	{
		return strcmp(trim(field1.plot_keywords), trim(field2.plot_keywords));
	}
	else if (fieldToBeAcc == 18)
	{
		return strcmp(trim(field1.movie_imdb_link), trim(field2.movie_imdb_link));
	}
	else if (fieldToBeAcc == 19)
	{
		if(field1.num_user_for_reviews < field2.num_user_for_reviews)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
	else if (fieldToBeAcc == 20)
	{
		return strcmp(trim(field1.language), trim(field2.language));
	}
	else if (fieldToBeAcc == 21)
	{
		return strcmp(trim(field1.country), trim(field2.country));
	}
	else if (fieldToBeAcc == 22)
	{
		return strcmp(trim(field1.content_rating), trim(field2.content_rating));
	}
	else if (fieldToBeAcc == 23)
	{
		if(field1.budget < field2.budget)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
	else if (fieldToBeAcc == 24)
	{
		if(field1.title_year < field2.title_year)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
	else if (fieldToBeAcc == 25)
	{
		if(field1.actor_2_facebook_likes < field2.actor_2_facebook_likes)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
	else if (fieldToBeAcc == 26)
	{
		if(field1.imdb_score < field2.imdb_score)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
	else if (fieldToBeAcc == 27)
	{
		if(field1.aspect_ratio < field2.aspect_ratio)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
	else /* field is 28  */
	{
		if(field1.movie_facebook_likes < field2.movie_facebook_likes)
		{
			return -1;
		}
		else
		{
			return 1;
		}
	}
  return 0;
}


void merge(struct movie_imdb_data * arr, int left, int mid, int right, int field) {
    int i, j, k;
    int temp1 = mid - left + 1;
    int temp2 =  right - mid;
 
    /* create temp arrays */
    struct movie_imdb_data * Left;
    struct movie_imdb_data * Right;
    
 	Left = (struct movie_imdb_data *)malloc(sizeof(struct movie_imdb_data) * (temp1 +1));
	Right = (struct movie_imdb_data *)malloc(sizeof(struct movie_imdb_data) * (temp1 +1));

    for (i = 0; i < temp1; i++)
    {
        Left[i] = arr[left + i];
    }

    for (j = 0; j < temp2; j++)
    {
        Right[j] = arr[mid + 1 + j];
    }
 
    i = 0; 
    j = 0; 
    k = left; 
    while (i < temp1 && j < temp2)
    {
    	if (compareArrayFields(Left[i],Right[j],field) < 0)
        {
            arr[k] = Left[i];
            i++;
        }
        else
        {
            arr[k] = Right[j];
            j++;
        }
        k++;
    }
    while (i < temp1)
    {
        arr[k] = Left[i];
        i++;
        k++;
    }

    while (j < temp2)
    {
        arr[k] = Right[j];
        j++;
        k++;
    }
    free(Left);
    free(Right);
}


void merge_sort(struct movie_imdb_data * arr, int left, int right, int field) {
  if (left < right) {
    int mid = left+(right-left)/2;
    merge_sort(arr, left, mid, field);
    merge_sort(arr, mid+1, right, field);
    merge(arr, left, mid, right, field);
  }
}

