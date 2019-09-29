#include "utils.h"
#include <pthread.h>

#define SERVER_PORT     6767
#define BUF_SIZE        2048
#define MAXHOSTNAMELEN  1024
#define MAX_STATION_ID   128
#define ITER_COUNT          1
#define GET_MODE           1
#define PUT_MODE           2
#define USER_MODE          3
#define BOTH_MODE          4
#define THREAD_NUM         10

struct sockaddr_in server_addr;                        // server_addr: The server address.

int get_station, 
    put_station = 0;

pthread_mutex_t put_mutx = PTHREAD_MUTEX_INITIALIZER,
                get_mutx = PTHREAD_MUTEX_INITIALIZER,
                socket_mutx = PTHREAD_MUTEX_INITIALIZER;


/**
 * @name print_usage - Prints usage information.
 * @return
 */
void print_usage() {
  fprintf(stderr, "Usage: client [OPTION]...\n\n");
  fprintf(stderr, "Available Options:\n");
  fprintf(stderr, "-h:             Print this help message.\n");
  fprintf(stderr, "-a <address>:   Specify the server address or hostname.\n");
  fprintf(stderr, "-o <operation>: Send a single operation to the server.\n");
  fprintf(stderr, "                <operation>:\n");
  fprintf(stderr, "                PUT:key:value\n");
  fprintf(stderr, "                GET:key\n");
  fprintf(stderr, "-i <count>:     Specify the number of iterations.\n");
  fprintf(stderr, "-g:             Repeatedly send GET operations.\n");
  fprintf(stderr, "-p:             Repeatedly send PUT operations.\n");
  fprintf(stderr, "-b:             Repeatedly send both PUT and GET operations.\n");
}

/**
 * @name talk - Sends a message to the server and prints the response.
 * @server_addr: The server address.
 * @buffer: A buffer that contains a message for the server.
 *
 * @return
 */
void talk(const struct sockaddr_in server_addr, char *buffer) {
  char rcv_buffer[BUF_SIZE];
  int socket_fd, numbytes;
      
  // create socket
  if ((socket_fd = socket(PF_INET, SOCK_STREAM, 0)) == -1) {
    ERROR("socket()");
  }

  // connect to the server.
  if (connect(socket_fd, (struct sockaddr*) &server_addr, sizeof(server_addr)) == -1) {
    ERROR("connect()");
  }
  
  // send message.
  write_str_to_socket(socket_fd, buffer, strlen(buffer));
      
  // receive results.
  printf("Result: ");
  do {
    memset(rcv_buffer, 0, BUF_SIZE);
    numbytes = read_str_from_socket(socket_fd, rcv_buffer, BUF_SIZE);
    if (numbytes != 0)
      printf("%s", rcv_buffer); // print to stdout
  } while (numbytes > 0);
  printf("\n");
      
  // close the connection to the server.
  close(socket_fd);
}

/**
 * @name put_operation - Sends PUT operation reapeatedly via threads.
 * @return
 */
void *put_operation(){
  int value;
  char buffer[BUF_SIZE];
  
  while(1){
    // create a random value.
    value = rand() % 65 + (-20);

    pthread_mutex_lock(&put_mutx);
    if(put_station > MAX_STATION_ID){
      break;
    }
    sprintf(buffer, "PUT:station.%d:%d", put_station, value);
    put_station++;
    pthread_mutex_unlock(&put_mutx);

    pthread_mutex_lock(&socket_mutx);
    printf("Operation: %s\n", buffer);
    talk(server_addr, buffer);                                                // O Buffer edw, einai Krisimos Poros.
    pthread_mutex_unlock(&socket_mutx);  
  }
  return NULL;
}

/**
 * @name get_operation - Sends GET operation reapeatedly via threads.
 * @return
 */
void *get_operation(){
  char buffer[BUF_SIZE];

  while(1){
    pthread_mutex_lock(&get_mutx);
    if(get_station > MAX_STATION_ID){
      break;
    }
    // Repeatedly GET.
    sprintf(buffer, "GET:station.%d", get_station);
    get_station++; 
    pthread_mutex_unlock(&get_mutx);
    
    pthread_mutex_lock(&socket_mutx);
    printf("Operation: %s\n", buffer);
    talk(server_addr, buffer);    
    pthread_mutex_unlock(&socket_mutx);
  }
  return NULL;
}

/*
 * @name threads_work - Create and join threads. Half threads will run PUT operation. The other half, GET.
 * @return
 */
void threads_work(){
  pthread_t tid[THREAD_NUM];
  int t;

  for(t=0; t<THREAD_NUM/2; t++){
    pthread_create(&tid[t], NULL, put_operation, NULL);  
  }
  for(t=THREAD_NUM/2; t<THREAD_NUM; t++){
    pthread_create(&tid[t], NULL, get_operation, NULL);  
  }

  for(t=0; t<THREAD_NUM; t++){
    pthread_join(tid[t], NULL);
  }
  fprintf(stdout, "\tAll threads Joined!\n");

  return;
}

/**
 * @name main - The main routine.
 */
int main(int argc, char **argv) {
  char *host = NULL;
  char *request = NULL;
  int mode = 0;
  int option = 0;
  int count = ITER_COUNT;
  char snd_buffer[BUF_SIZE];
  int station, value;
  struct hostent *host_info;
  
  // Parse user parameters.
  while ((option = getopt(argc, argv,"i:hgpbo:a:")) != -1) {
    switch (option) {
      case 'h':
        print_usage();
        exit(0);
      case 'a':
        host = optarg;
        break;
      case 'i':
        count = atoi(optarg);
	break;
      case 'g':
        if (mode) {
          fprintf(stderr, "You can only specify one of the following: -g, -p, -b, -o\n");
          exit(EXIT_FAILURE);
        }
        mode = GET_MODE;
        break;
      case 'p': 
        if (mode) {
          fprintf(stderr, "You can only specify one of the following: -g, -p, -b, -o\n");
          exit(EXIT_FAILURE);
        }
        mode = PUT_MODE;
        break;
      case 'b':         // Both PUT & GET requests
        if (mode) {
          fprintf(stderr, "You can only specify one of the following: -g, -p, -b, -o\n");
          exit(EXIT_FAILURE);
        }
        mode = BOTH_MODE;
        break;
      case 'o':
        if (mode) {
          fprintf(stderr, "You can only specify one of the following: -r, -w, -o\n");
          exit(EXIT_FAILURE);
        }
        mode = USER_MODE;
        request = optarg;
        break;
      default:
        print_usage();
        exit(EXIT_FAILURE);
    }
  }

  // Check parameters.
  if (!mode) {
    fprintf(stderr, "Error: One of -g, -p, -b, -o is required.\n\n");
    print_usage();
    exit(0);
  }
  if (!host) {
    fprintf(stderr, "Error: -a <address> is required.\n\n");
    print_usage();
    exit(0);
  }
  
  // get the host (server) info
  if ((host_info = gethostbyname(host)) == NULL) { 
    ERROR("gethostbyname()"); 
  }
    
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr = *((struct in_addr*)host_info->h_addr);
  server_addr.sin_port = htons(SERVER_PORT);

  if (mode == USER_MODE) {
    memset(snd_buffer, 0, BUF_SIZE);
    strncpy(snd_buffer, request, strlen(request));
    printf("Operation: %s\n", snd_buffer);
    talk(server_addr, snd_buffer);
  } else {
    while(--count>=0) {
      for (station = 0; station <= MAX_STATION_ID; station++) {
        memset(snd_buffer, 0, BUF_SIZE);
        if (mode == GET_MODE) {
          // Repeatedly GET.
          sprintf(snd_buffer, "GET:station.%d", station);
        } else if (mode == PUT_MODE) {
          // Repeatedly PUT.
          // create a random value.
          value = rand() % 65 + (-20);
          sprintf(snd_buffer, "PUT:station.%d:%d", station, value);
        } else if (mode == BOTH_MODE){                               // Send both GET & PUT requests, using threads.
          threads_work();
          break;
        }
        printf("Operation: %s\n", snd_buffer);
        talk(server_addr, snd_buffer);
      }
    }
  }
  return 0;
}

