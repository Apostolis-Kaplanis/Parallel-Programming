/* server.c

   Sample code of 
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina 

   (c) S. Anastasiadis, G. Kappes 2016

*/

#include <pthread.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/time.h>
#include "utils.h"
#include "kissdb.h"

#define MY_PORT                 6767
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10

#define QUEUE_SIZE                 10  // QUEUE_SIZE>=2
#define THREAD_NUM                 10  // THREAD_NUM>=1

#define EMPTY                      1   // FIFO Queue's states
#define FULL                       2
#define LOADED                     3


// Definition of the operation type.
typedef enum operation {
  PUT,
  GET
} Operation; 

// Definition of the request.
typedef struct request {
  Operation operation;
  char key[KEY_SIZE];  
  char value[VALUE_SIZE];
} Request;

// Definition of FIFO's elements
typedef struct inqueue
{ 
  int accptFd;                 // File Descriptor
  struct timeval accptTime;    // Time (sec,usecs)
} InQueue;

pthread_t id[THREAD_NUM];      // All threads

int state;                     // FIFO Queue's state. (defined as EMPTY, FULL or LOADED)
int head, tail =0;             // FIFO Queue's head & tail.

double total_waiting_time,            
       total_service_time = 0.0;
int completed_requests = 0;

pthread_mutex_t fifo_mutx = PTHREAD_MUTEX_INITIALIZER,
                times_mutx = PTHREAD_MUTEX_INITIALIZER,
                put_critical = PTHREAD_MUTEX_INITIALIZER;

pthread_cond_t emptyFifo = PTHREAD_COND_INITIALIZER,
               fullFifo = PTHREAD_COND_INITIALIZER;

int reader_count,                   // Count readers(GET), writers(PUT)
    writer_count = 0;

// Definition of the database.
KISSDB *db = NULL;

InQueue aithseis[QUEUE_SIZE];       // FIFO Queue's array.

/**
 * @name parse_request - Parses a received message and generates a new request.
 * @param buffer: A pointer to the received message.
 *
 * @return Initialized request on Success. NULL on Error.
 */
Request *parse_request(char *buffer) {
  char *token = NULL;
  Request *req = NULL;
  
  // Check arguments.
  if (!buffer)
    return NULL;
  
  // Prepare the request.
  req = (Request *) malloc(sizeof(Request));
  memset(req->key, 0, KEY_SIZE);
  memset(req->value, 0, VALUE_SIZE);

  // Extract the operation type.
  token = strtok(buffer, ":");    
  if (!strcmp(token, "PUT")) {
    req->operation = PUT;
  } else if (!strcmp(token, "GET")) {
    req->operation = GET;
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the key.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->key, token, KEY_SIZE);
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the value.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->value, token, VALUE_SIZE);
  } else if (req->operation == PUT) {
    free(req);
    return NULL;
  }
  return req;
}

/*
 * @name process_request - Process a client request.
 * @param socket_fd: The accept descriptor.
 *
 * @return
 */
void *process_request() {
  char response_str[BUF_SIZE], request_str[BUF_SIZE];
  int numbytes = 0;
  Request *request = NULL;

  struct timeval temp,
                 getTime1,
                 getTime2;          // Time variables.

  int socket_fd;                    
  double xronos_anamonhs,           // O xronos pou paremeine h aithsh mesa sth FIFO oura, mexri na ksekinhsei h anazhthsh (sthn KISSDB)
         xronos_eksyphrethshs;      // O xronos pou apaiththhke gia thn anazhthsh ths lekshs se ola ta arxeia (ths KISSDB)

  // Note: Ta threads tha'Epanaxrhsimopoiountai'. Gia na mhn termatizoun otan oloklhrwsoun thn synarthh tous, tha trexoun se brogxo.
  while(1){
    pthread_mutex_lock(&fifo_mutx);

    while(head==tail){              // FIFO Queue is empty.   Note: Always 'while' at Conditions here, to avoid unwanted problems.  (Never 'if')
      state=EMPTY;

      // Note: Wait mexri na erthei (h prwth)aithsh apo Master-Thread. (wait, wste na mhn trexoun askopa ta threads)
      pthread_cond_wait(&emptyFifo, &fifo_mutx);  
    }
    
    // Extract from FIFO.
    socket_fd = aithseis[head].accptFd;
    temp = aithseis[head].accptTime;

    // Move forward FIFO's Head, after extraction:
    head++;
    if(head>QUEUE_SIZE){            // Reset head back at start of FIFO Queue.
      head=0;
    }

    // Eyresh Xronou-Anamonhs:
    gettimeofday(&getTime1, NULL);
    xronos_anamonhs = (getTime1.tv_sec - temp.tv_sec)*1000000 +              // convert sec to μsec (1 sec = 10^6 usec),     //*1.0E-6
                      (getTime1.tv_usec - temp.tv_usec);          
    fprintf(stdout, "THREAD_in_func (id):: %ld\n", pthread_self());

    pthread_cond_signal(&fullFifo);           // Signal Master-Thread that Fifo Queue is't Full. (New (waiting)accepts can finally come)
    pthread_mutex_unlock(&fifo_mutx);


    // Clean buffers.
    memset(response_str, 0, BUF_SIZE);
    memset(request_str, 0, BUF_SIZE);
    
    // receive message.
    numbytes = read_str_from_socket(socket_fd, request_str, BUF_SIZE);
    
    // parse the request.
    if (numbytes) {
      request = parse_request(request_str);
      if (request) {
        switch (request->operation) {
          case GET:                 // Readers      
            
            // Read the given key from the database.
            if (KISSDB_get(db, request->key, request->value))
              sprintf(response_str, "GET ERROR\n");
            else
              sprintf(response_str, "GET OK: %s\n", request->value);

            break;
          case PUT:                 // Writers
            
            pthread_mutex_lock(&put_critical);                // ENAS grafeas sthn KISSDB.
            // Write the given key/value pair to the database.
            if (KISSDB_put(db, request->key, request->value)) 
              sprintf(response_str, "PUT ERROR\n");
            else
              sprintf(response_str, "PUT OK\n");
            pthread_mutex_unlock(&put_critical);
  
            break;
          default:
            // Unsupported operation.
            sprintf(response_str, "UNKOWN OPERATION\n");
        }
        // Reply to the client.
        write_str_to_socket(socket_fd, response_str, strlen(response_str));

        // close fd:
        close(socket_fd);

        fprintf(stdout, "response: %s\n", response_str);

        if (request)
          free(request);
        request = NULL;

        // Eyresh Xronou-Eksyphrethshs:
        gettimeofday(&getTime2, NULL);
        xronos_eksyphrethshs = (getTime2.tv_sec - getTime1.tv_sec)*1000000 +        // convert sec to μsec (1 sec = 10^6 usec)
                               (getTime2.tv_usec - getTime1.tv_usec);

        // Enhmerwsh koinoxrhstwn metablhtwn:
        pthread_mutex_lock(&times_mutx);   
        total_waiting_time += xronos_anamonhs;
        total_service_time += xronos_eksyphrethshs;
        completed_requests += 1;
        pthread_mutex_unlock(&times_mutx);
      }
      else{                                                                     // When request (struct: Operation(PUT/GET), key, value) isn't at correct format. 
        // Send an Error reply to the client.
        sprintf(response_str, "FORMAT ERROR\n");
        write_str_to_socket(socket_fd, response_str, strlen(response_str));

        //closer fd:
        close(socket_fd);
      }
    }
    
  }
  return NULL;    // To pass warning.
}

/*
 * @name threads_consumers - Creating threads.
 * @return
 */
void threads_consumers(){
  int k, rc;

  for(k=0; k<THREAD_NUM; k++){
    rc = pthread_create(&id[k], NULL, process_request, NULL);                     // Note: To 2o orisma, ayto tou Atrribute, einai NULL: 'joinable' by default.
    fprintf(stdout, "Thread(%d/%d) created \t[id: %ld]\n", k+1, THREAD_NUM, id[k]);
    if(rc){
      fprintf(stdout, "ERROR; return code from pthread_create is: %d\n", rc);
      exit(-1);
    }
  }
  return;
}

/*
 * @name statistics_handler - Print statistics and terminate the program.
 * @return
 */
void statistics_handler(){
  double avgWaitingTime, avgServiceTime;
  //int t, rc;

  avgWaitingTime = total_waiting_time/completed_requests;
  avgServiceTime = total_service_time/completed_requests;

  fprintf(stdout, "\nSignal-> 'Control+Z': program exit, print statistics:\n\tcompleted-requests: %5d\n\tavg-waiting-time: %5lf usecs\n\tavg-service-time: %5lf usecs\t (1sec = 10^6usecs)\n", completed_requests, avgWaitingTime, avgServiceTime);
 
  /* 
    Note: Ta threads se aythn thn periptwsh den mas endiaferoun na ginoun join (na apodesmeysoun tous porous tous). 
          Dhladh otan o Server termatisei, ta nhmata tha termatisoun me to kleisimo ths diergasias pou ta dhmiourghse, apodesmeyontas tous porous tous.
  
    for(t=0; t<THREAD_NUM; t++){
      rc = pthread_join(id[t], NULL);
      if(rc){
        fprintf(stdout, "ERROR; return code from pthread_join() is: %d\n", rc);
        exit(-1);
      }
      fprintf(stdout, "thread(%d/%d) [id: %ld] joined.\n", t+1, THREAD_NUM, id[t]);
    }
    fprintf(stdout, "Ok, threads joined.\n");

    Note: Edw yparxei Segmentation fault(core dumped), kathws to Prwto thread exei 'ksafnika' diaforetiko id apo ayto pou dhmiourghthke!
  */

  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Program exits normally.
  exit(0);
  return;
}

/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */
int main() {
  struct timeval tv;

  int socket_fd,                    // listen on this socket for new connections
      new_fd;                       // use this socket to service a new connection
  socklen_t clen;
  struct sockaddr_in server_addr,   // my address information
                     client_addr;   // connector's address information

  fprintf(stdout, "\n\t~(help) Server's proc_id : '%d'\n\t\tuse: 'kill -9 -[proc_id]',  to teminate this process,\n", getpid());
  fprintf(stdout, "\t\t     'ps -f' to find it.\n");

  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");

  // Ignore the SIGPIPE signal in order to not crash when a
  // client closes the connection unexpectedly.
  signal(SIGPIPE, SIG_IGN);

  // When Control+Z is pressed, handler 'statistics_handler' is called.
  signal(SIGTSTP, statistics_handler);
  
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);    // any local interface
  server_addr.sin_port = htons(MY_PORT);
  
  // bind socket to address
  if (bind(socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1)
    ERROR("bind()");
  
  // start listening to socket for incomming connections
  listen(socket_fd, MAX_PENDING_CONNECTIONS);
  fprintf(stderr, "(Info) main: Listening for new connections on port %d ...\n", MY_PORT);
  clen = sizeof(client_addr);

  //fprintf(stdout, "\n\t~Listening fd (server's fd): \t%d\n", socket_fd);

  // Allocate memory for the database.
  if (!(db = (KISSDB *)malloc(sizeof(KISSDB)))) {
    fprintf(stderr, "(Error) main: Cannot allocate memory for the database.\n");
    return 1;
  }
  
  // Open the database.
  if (KISSDB_open(db, "mydb.db", KISSDB_OPEN_MODE_RWCREAT, HASH_SIZE, KEY_SIZE, VALUE_SIZE)) {
    fprintf(stderr, "(Error) main: Cannot open the database.\n");
    return 1;
  }

  // Creating threads.
  threads_consumers();

  state=EMPTY;              // FIFO Queue is empty at the beggining.

  // main loop: wait for new connection/requests
  while (1) { 
    // wait for incomming connection
    if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
      ERROR("accept()");
    }
    //fprintf(stdout, "\t~Server's 'new_fd' (for this client) : %d\n", new_fd);
    
    // got connection, serve request
    fprintf(stderr, "(Info) main: Got connection from '%s'\n", inet_ntoa(client_addr.sin_addr));

    //lock tail
    pthread_mutex_lock(&fifo_mutx);                 // Note: Idia metablhth me to lock ths process_request gia na mhn 'peirazoun' taytoxrona thn FIFO.
    if(state!=FULL){                               // FIFO Queue isn't full. (It's empty or loaded).
      // Apothkeysh stoixeiwn ths kathe Aithshs (pou hrthe me accept()) sthn FIFO. (struct: File-Descriptor, Time)
      aithseis[tail].accptFd = new_fd;
      gettimeofday(&tv, NULL);
      aithseis[tail].accptTime = tv;
      //fprintf(stdout, "\t~Nea Aithsh:  FileDescr: %d\n", aithseis[tail].accptFd);
      
      tail++;                       // Proxwrw to 'tail' mia thesh mprosta gia thn epomenh(available) apothhkeysh ths Aithshs.
      state=LOADED;
    }

    if(tail>QUEUE_SIZE){
      tail=0;                       // Reset tail back at start of FIFO.
    }

    while(tail==head){              // FIFO is full.       Note: Always 'while' at Conditions here, to avoid unwanted problems. (Never 'if')
      state=FULL;
      fprintf(stdout, "(FIFO is Full) waiting for empty slot in FIFO...\n");

      // Note: To Master-Thread tha kanei Wait otan h FIFO einai Full, mexri na erthei Signal apo ta threads oti yparxei eleytheros xwros sth FIFO.
      pthread_cond_wait(&fullFifo, &fifo_mutx);
    }
    // Signal threads to continue. (Not Empty Fifo now)
    pthread_cond_signal(&emptyFifo);

    //unlock tail
    pthread_mutex_unlock(&fifo_mutx);
  }  

  // Destroy the database.
  // Close the database.
  KISSDB_close(db);

  // Free memory.
  if (db)
    free(db);
  db = NULL;

  return 0; 
}

