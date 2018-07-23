#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>

#include<pthread.h>
#include <semaphore.h>

#include <netdb.h>
#include <arpa/inet.h> 
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "sorter_client.h"

#define TOKENS_IN_HEADER 28

const char * header[TOKENS_IN_HEADER] = { "color", "director_name" ,"num_critic_for_reviews", "duration", "director_facebook_likes",                  "actor_3_facebook_likes","actor_2_name","actor_1_facebook_likes","gross,genres","actor_1_name","movie_title",
"num_voted_users","cast_total_facebook_likes","actor_3_name","facenumber_in_poster","plot_keywords",
"movie_imdb_link","num_user_for_reviews","language","country","content_rating","budget","title_year","actor_2_facebook_likes","imdb_score",
"aspect_ratio","movie_facebook_likes" };

int get_field_number(const char * header[TOKENS_IN_HEADER], const char * field_name) {
  int i = 0;

  for (i = 0; i < TOKENS_IN_HEADER; ++i) {
    if (strcmp(header[i], field_name) == 0) return i + 1;
  }

  return -1;
}

int main(int argc, char ** argv) {
  int i; 
  char * host = "";
  int port, session_id, files_num;
  char ** csv_files = NULL;
  char in_dir[MAX_FILE_NAME] = ".";
  char out_dir[MAX_FILE_NAME] = ".";
  pthread_t * workers;
  worker_args_t * w_args;
  int field = -1;
  char field_name[COLUMN_TO_SORT_NAME_LENGTH] = "";
  
  if (argc < 7) {
    printf("Usage: ./client <-c> <column> <-h> <host> <-p> port\n");
    return 0;
  }

  if (strcmp(argv[1], "-c") != 0) {
    printf("Column not defined\n");
    return 0;
  }

  strcpy(field_name, argv[2]);

  field = get_field_number(header, field_name);
  if (field <= 0) {
    printf("Error occurred while trying to get sort column number from header\n");
         return 0;
  }

  if (strcmp(argv[3], "-h") != 0) {
    printf("Host name not defined\n");
    return 0;
  }

  host = host_ip(argv[4]);

  if (strcmp(argv[5], "-p") != 0) {
    printf("Port not defined\n");
    return 0;
  }

  port = atoi(argv[6]);

  if (argc > 8 && strcmp(argv[7], "-d") == 0) {
    strcpy(in_dir, argv[8]);
  }

  if (argc > 10 && strcmp(argv[9], "-o") == 0) {
    strcpy(out_dir, argv[10]);
  }

  if (strcmp(in_dir, ".") == 0 && getcwd(in_dir, MAX_FILE_NAME) == NULL) {
    printf("Error! No possible to get current working directory\n");
    return 0;
  }

  if (strcmp(out_dir, ".") == 0 && getcwd(out_dir, MAX_FILE_NAME) == NULL) {
    printf("Error! No possible to get current working directory\n");
    return 0;
  }

  session_id = get_session_id(host, port);
  if (session_id < 0) {
     printf("Error! Session id not received from server\n");
     return 0;
  }
  
  /*printf("Session id is: %d\n", session_id);*/

  csv_files = get_csv_files(in_dir, &files_num);
  if (files_num == 0) {
    printf("No csv files found\n");
    return 0;
  }

  workers = malloc(sizeof(pthread_t) * files_num);
  if (!workers) {
    printf("Error! Cannot allocate memory for workers threads\n");
    return 0;
  }

  for (i = 0; i < files_num; ++i) {
    w_args = malloc(sizeof(worker_args_t));
    if (w_args) {
      strcpy(w_args->host, host);
      w_args->port = port;
      w_args->session_id = session_id;
      w_args->filename = csv_files[i];
      w_args->field = field; 
    }
    pthread_create(&workers[i], NULL, worker_job, w_args);
  }
  
  for (i = 0; i < files_num; ++i) {
    pthread_join(workers[i], NULL);
  }

  get_sorted_data(host, port, session_id, field, out_dir);

  /* deallocate occupied memory */
  for (i = 0; i < files_num; ++i) {
    free(csv_files[i]);
  }
  free(csv_files);
  free(workers);

  return 0;
}

void get_sorted_data(const char * host, int port, int session_id, int field, const char * out_dir) {
  int sock = create_socket(host, port);
  char * request;
  char buffer[MAX_MESSAGE_LENGTH + 1];
  FILE * fout;

  if (sock == -1) {
    printf("Error! Cannot get socket to ask sorted data\n");
    return;
  }
  sprintf(buffer, "%*d", -MAX_FIELD_LENGTH, field);
  buffer[MAX_MESSAGE_LENGTH] = '\0';
  request = construct_request(DUMP_REQUEST, session_id, buffer, END_OF_MESSAGE);
  if (!request) {
    printf("Error! Cannot allocate memory for send request\n");
    return;
  }

  if (!send_log_if_error(sock, request, "Cannot send dump request")) {
    /* handle send error */
    free(request);
    close(sock);
    return;
  }
      
  free(request);

  sprintf(buffer, "%s/AllFiles-sorted.csv", out_dir);
  fout = fopen("AllFiles-sorted.csv", "w");
  if (!fout) {
    printf("Cannot create file for writing %s\n", buffer);
    close(sock);
    return;
  }

  printf("Creating %s ...\n", buffer);

  fprintf(fout, "%s\n", "color,director_name,num_critic_for_reviews,duration,director_facebook_likes,actor_3_facebook_likes,actor_2_name,actor_1_facebook_likes,gross,genres,actor_1_name,movie_title,num_voted_users,cast_total_facebook_likes,actor_3_name,facenumber_in_poster,plot_keywords,movie_imdb_link,num_user_for_reviews,language,country,content_rating,budget,title_year,actor_2_facebook_likes,imdb_score,aspect_ratio,movie_facebook_likes");
  do {
   if (read(sock, buffer, MAX_MESSAGE_LENGTH) <= 0) {
     break;
   } else {
      fprintf(fout, "%s", buffer);
   }

   request = construct_request(DUMP_REQUEST, session_id, "OK", END_OF_MESSAGE);
   if (!request) {
     printf("Error! Cannot allocate memory for send request\n");
      return;
   }
   if (!send_log_if_error(sock, request, "Cannot send dump reply")) {
     free(request);
     break;
   }
   free(request);
  } while(TRUE);


  close(sock);
  fclose(fout);
}

void * worker_job(void * args) {
  worker_args_t * w_args = (worker_args_t *)args;
  const char * file;
  FILE * fin;
  int i = 0, length = 0;
  char buffer[MAX_MESSAGE_LENGTH + 1] = "";
  char request_storage[MAX_MESSAGE_LENGTH + 1] = "";
  int sock, j;
  char * request;
  char field_buffer[MAX_FIELD_LENGTH + 1] = "";
  
  if (!w_args) return NULL;
  file = w_args->filename;
  if (!file) return NULL;
  
  fin = fopen(file, "r");
  if (!fin) {
   printf("Error! Cannot open '%s' for reading\n", file);
   return NULL;
  }

  sock = create_socket(w_args->host, w_args->port);
  if (sock == -1) {
    printf("Error! Cannot create socket for sending %s content\n", file);
    return NULL;
  }

  sprintf(field_buffer, "%*d", -MAX_FIELD_LENGTH, w_args->field);

  while(sock != -1 && fgets(buffer, MAX_MESSAGE_LENGTH, fin)) {
    length = strlen(buffer);
    
    /* trim line ending */
    for (j = length - 1; j >=0; --j) {
      if (buffer[j] == '\n') {
        buffer[j] = '\0';
      } else break;
    }
    
    /*printf("Record is: '%s'\n", buffer);*/

    /* not heading */
    if (i != 0) {
      sprintf(request_storage, "%*s%s", -COLUMN_TO_SORT_NAME_LENGTH, field_buffer, buffer);
      request = construct_request(SORT_REQUEST, w_args->session_id, request_storage, "");
      if (!request) {
        printf("Error! Cannot allocate memory for send request\n");
        break;
      }
/*      printf("Request is '%s'\n", request); */
      if (!send_log_if_error(sock, request, "Cannot send file's content")) {
      /* handle send error */
        free(request);
        break;
      }
      
      free(request);

      if (!get_reply(sock, buffer)) {
        printf("Error! No confirmation from server\n");
        break;
      }   
    }
    i += 1;
  }

  request = construct_request(SORT_REQUEST, w_args->session_id, field_buffer, END_OF_MESSAGE);
  send_log_if_error(sock, request, "Cannot send finish for sorting\n");
  free(request);

  close(sock);
  fclose(fin);
  free(w_args);

  return NULL;
}

char ** append_string(char ** stringlist, int * size, int * capacity, const char * str) {
  char ** tmp = stringlist;
  int next_capacity, i;

  if (*size + 1 >= *capacity) {
    next_capacity = *capacity + 10;
    stringlist = malloc(sizeof(char **) * next_capacity);
    if (!stringlist) return tmp;
    for (i = 0; i < *capacity; ++i) {
      stringlist[i] = tmp[i];
      tmp[i] = NULL;
    }
    *capacity = next_capacity;
    free(tmp);
  }

  stringlist[*size] = malloc(sizeof(char *) * (strlen(str) + 1));
  if (stringlist[*size]) {
    strcpy(stringlist[*size], str);
  }

  *size += 1;
  return stringlist;
}

char ** get_csv_files(const char * directory, int * files_num) {
  char ** result = NULL; 
  char ** dirs = NULL;
  int dirs_num = 0;
  int f_capacity = 0, d_capacity = 0;
  int dirs_processed = 0;
  const char * d_name, * extension;

  DIR *d;
  struct dirent *dir;
  char path[MAX_FILE_NAME];

  *files_num = 0;

  dirs = append_string(dirs, &dirs_num, &d_capacity, directory);
  while (dirs_num > dirs_processed) {
    d_name = dirs[dirs_processed];
    d = opendir(d_name);
    if (d) {
      while ((dir = readdir(d)) != NULL) {
        if (strcmp(dir->d_name, ".") == 0) continue;
        if (strcmp(dir->d_name, "..") == 0) continue;

        extension = strstr(dir->d_name, ".csv");
        /* add csv file to storage */
        if (extension && strlen(extension) == 4) {
          sprintf(path, "%s/%s", d_name, dir->d_name);
          result = append_string(result, files_num, &f_capacity, path);
        } else if (dir->d_type == DT_DIR) { /* add directory to search */
          sprintf(path, "%s/%s", d_name, dir->d_name);
          dirs = append_string(dirs, &dirs_num, &d_capacity, path);
        }
      }
      closedir(d);
    }
    free(dirs[dirs_processed]);
    dirs[dirs_processed++] = NULL;
  }

  free(dirs);
  return result;
}

char * host_ip(const char * hostname) {
  int i;
  struct hostent * host = gethostbyname(hostname);
  struct in_addr * addr = NULL;
  if (!host) return NULL;
  for (i = 0; host->h_addr_list[i]; ++i) {
    if ((addr = (struct in_addr*)host->h_addr_list[i])) break;
  }
  
  if (!addr) return NULL;
  return inet_ntoa(*addr);
}

int create_socket(const char * address, int port) {
  int sockfd;
  struct sockaddr_in serv_addr;

  if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) return sockfd;

  memset(&serv_addr, '0', sizeof(serv_addr)); 
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(port); 

  if (inet_pton(AF_INET, address, &serv_addr.sin_addr) <=0) return -1;
  if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) return -1;

  return sockfd;
}

char * construct_request(const char * command, int session_id, const char * argument, const char * end_msg) {
  int length = MAX_REQUEST_TYPE_LENGTH + strlen(argument) + strlen(end_msg) + SESSION_ID_LENGTH;
  char * request = malloc(sizeof(char) * (length + 1));
  if (request) {
    sprintf(request, "%*s%*d%s%s", -MAX_REQUEST_TYPE_LENGTH, command,
            -SESSION_ID_LENGTH, session_id, argument, end_msg);
  }
  return request;
}

BOOLEAN send_log_if_error(int sock, const char * request, const char * error_message) {
  if (send(sock, request, strlen(request) + 1, 0) == -1) {
    printf("Error! %s\n", error_message);
    return FALSE;
  }
  return TRUE;
}

BOOLEAN get_reply(int sock, char buffer[MAX_MESSAGE_LENGTH]) {
  int n;
  char * request_end;

  n = read(sock, buffer, MAX_MESSAGE_LENGTH);
  if (n <= 0) return FALSE;

  request_end = strstr(buffer, END_OF_MESSAGE);
  if (!request_end) return FALSE;  
  /*replace end by \0 */
  request_end[0] = '\0';

  return TRUE;
}

int get_session_id(const char * address, int port) {
  int sock = create_socket(address, port);
  char buffer[MAX_MESSAGE_LENGTH + 1];
  char * request;
  int session = -1;
  
  do {
    if (sock == -1) break;

    request = construct_request(GET_ID_REQUEST, -1, "", END_OF_MESSAGE);
    if (!request) {
      printf("Error! Cannot allocate memory for send request\n");
      break;
    }

    if (send_log_if_error(sock, request, "Cannot send session request")) {
      if (get_reply(sock, buffer)) {
        session = atoi(buffer);
      }
    }

    free(request);
    close(sock);
  } while (FALSE);

  return session;
}
