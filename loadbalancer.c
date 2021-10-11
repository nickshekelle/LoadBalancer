#include <getopt.h>
#include<err.h>
#include<arpa/inet.h>
#include<netdb.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<unistd.h>
#include <pthread.h>
#include <limits.h>
#include <ctype.h>



pthread_mutex_t q_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t q_cond_var = PTHREAD_COND_INITIALIZER;

pthread_mutex_t total_req_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t total_req_cond_var = PTHREAD_COND_INITIALIZER;

pthread_mutex_t healthcheck_vals_lock = PTHREAD_MUTEX_INITIALIZER;


typedef struct {
    int *client_socket;
    int *server_socket;
} Tuple; // for tuples

typedef struct {
    int requests;
    int errors;
} HealthTuple; // for tuples

typedef struct {
    int total_req;
    int time_value;
    HealthTuple* healthcheck_vals;
    uint16_t *server_ports;
    int num_servers;
} Globals; // for tuples

struct node {
    struct node* next;
    Tuple *client_server_pair; //will hold the accepted connection
};

typedef struct node node_t;

node_t *head = NULL;
node_t *tail = NULL;

//queue functions
void enqueue(int *client_socket, int *server_socket){
    node_t *newnode = malloc(sizeof(node_t));
    newnode->client_server_pair = malloc(sizeof(Tuple));
    newnode->client_server_pair->client_socket = client_socket;
    newnode->client_server_pair->server_socket = server_socket;
    newnode->next = NULL;
    if(tail == NULL){
        head = newnode;
    }else{
        tail->next = newnode;
    }
    tail = newnode;
}


Tuple* dequeue(){ //returns pointer to socket
    if(head == NULL){
        return NULL;
    }else{
        Tuple *result = head->client_server_pair;
        node_t *temp = head;
        head = head->next;
        if(head == NULL){
            tail = NULL;
        }
        free(temp);
        return result;
    }
}




int start_of_content(char* string){
    char* flag = "\r\n\r\n";
    int i=0,k=0,c,index=-5;
    while(string[i]!='\0'){
        if(string[i]==flag[0]){
            k=1;
            for(c=1;flag[c]!='\0';c++){
                if(string[i+c]!=flag[c]){
                 k=0;
                 break;
                }
            }
        }
        if(k==1){
            index=i;
        }
        i++;
        k=0;
    }
    return index + 4; //to account for the 4 leading chars (\r\n\r\n)
    // returns -1 if flag not found
}


int content_length(char* string){
    char* save_ptr;
    char request_copy[5000];
    strcpy(request_copy, string);
    char* request = strtok_r(request_copy, "\n", &save_ptr);
    while(strstr(request, "Content-Length:") == 0){
        request = strtok_r(NULL, "\n", &save_ptr);

    }
    return atoi(request+16);
}


/*
 * client_connect takes a port number and establishes a connection as a client.
 * connectport: port number of server to connect to
 * returns: valid socket if successful, -1 otherwise
 */
int client_connect(uint16_t connectport) {
    int connfd;
    struct sockaddr_in servaddr;

    connfd=socket(AF_INET,SOCK_STREAM,0);
    if (connfd < 0)
        return -1;
    memset(&servaddr, 0, sizeof servaddr);

    servaddr.sin_family=AF_INET;
    servaddr.sin_port=htons(connectport);

    /* For this assignment the IP address can be fixed */
    inet_pton(AF_INET,"127.0.0.1",&(servaddr.sin_addr));

    if(connect(connfd,(struct sockaddr *)&servaddr,sizeof(servaddr)) < 0)
        return -1;
    return connfd;
}

/*
 * server_listen takes a port number and creates a socket to listen on
 * that port.
 * port: the port number to receive connections
 * returns: valid socket if successful, -1 otherwise
 */
int server_listen(int port) {
    int listenfd;
    int enable = 1;
    struct sockaddr_in servaddr;

    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0)
        return -1;
    memset(&servaddr, 0, sizeof servaddr);
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htons(INADDR_ANY);
    servaddr.sin_port = htons(port);

    if(setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) < 0)
        return -1;
    if (bind(listenfd, (struct sockaddr*) &servaddr, sizeof servaddr) < 0)
        return -1;
    if (listen(listenfd, 500) < 0)
        return -1;
    return listenfd;
}

/*
 * bridge_connections send up to 4080 bytes from fromfd to tofd
 * fromfd, tofd: valid sockets
 * returns: number of bytes sent, 0 if connection closed, -1 on error
 */
int bridge_connections(int fromfd, int tofd) {
    char recvline[4080];
    int n = recv(fromfd, recvline, 4080, 0);
    if (n < 0) {
        printf("connection error receiving\n");
        return -1;
    } else if (n == 0) {
        printf("receiving connection ended\n");
        return 0;
    }
    recvline[n] = '\0';
    n = send(tofd, recvline, n, 0);
    if (n < 0) {
        printf("connection error sending\n");
        return -1;
    } else if (n == 0) {
        printf("sending connection ended\n");
        return 0;
    }
    return n;
}

/*
 * bridge_loop forwards all messages between both sockets until the connection
 * is interrupted. It also prints a message if both channels are idle.
 * sockfd1, sockfd2: valid sockets
 */
void bridge_loop(int sockfd1, int sockfd2) {
    fd_set set;
    struct timeval timeout;

    int fromfd, tofd;
    while(1) {
        // set for select usage must be initialized before each select call
        // set manages which file descriptors are being watched
        FD_ZERO (&set);
        FD_SET (sockfd1, &set);
        FD_SET (sockfd2, &set);

        // same for timeout
        // max time waiting, 5 seconds, 0 microseconds
        timeout.tv_sec = 5;
        timeout.tv_usec = 0;

        // select return the number of file descriptors ready for reading in set
        switch (select(FD_SETSIZE, &set, NULL, NULL, &timeout)) {
            case -1:
                printf("error during select, exiting\n");
                return;
            case 0:
                printf("both channels are idle, assume server taking too long to respond\n");
                return;
            default:
                if (FD_ISSET(sockfd1, &set)) {
                    fromfd = sockfd1;
                    tofd = sockfd2;
                } else if (FD_ISSET(sockfd2, &set)) {
                    fromfd = sockfd2;
                    tofd = sockfd1;
                } else {
                    printf("this should be unreachable\n");
                    return;
                }
        }

        if (bridge_connections(fromfd, tofd) <= 0)
            return;
    }
}


void * look_for_work(void* arg){
    char c[1000];
    while(1){
        Tuple *client_server_pair;
        pthread_mutex_lock(&q_lock);
        client_server_pair = dequeue();
        while(client_server_pair == NULL){
            pthread_cond_wait(&q_cond_var, &q_lock);
            client_server_pair = dequeue();
        }
        pthread_mutex_unlock(&q_lock);
        if(client_server_pair != NULL){ // we found work
            bridge_loop(*client_server_pair->client_socket, *client_server_pair->server_socket);
        }
        close(*client_server_pair->client_socket);
        close(*client_server_pair->server_socket);
        free(client_server_pair->client_socket);
        free(client_server_pair->server_socket);
        free(client_server_pair);
    }
}


void probe_server(Globals *globals, int initial){

    if(initial != 1){
        struct timespec timeval;
        clock_gettime(CLOCK_REALTIME, &timeval);
        timeval.tv_sec += globals->time_value;
        pthread_mutex_lock(&total_req_lock);
        pthread_cond_timedwait(&total_req_cond_var, &total_req_lock, &timeval);
        pthread_mutex_unlock(&total_req_lock);
    }

    // probe servers
    printf("healthcheck probe\n");
    for(int i = 0; i<globals->num_servers; i++){
        // for all servers, create a socket to server
        int socket = client_connect(globals->server_ports[i]);
        // if server is down
        if(socket == -1){
            pthread_mutex_lock(&healthcheck_vals_lock);
            globals->healthcheck_vals[i].requests = INT_MAX;
            globals->healthcheck_vals[i].errors = INT_MAX;
            pthread_mutex_unlock(&healthcheck_vals_lock);
            printf("Server at %d down\n", globals->server_ports[i] );
            continue;
        }

        write(socket, "GET /healthcheck HTTP/1.1\r\n\r\n", 29);

        //wait until data is in the socket (timeout check)
        fd_set set;
        struct timeval timeout;
        FD_ZERO (&set);
        FD_SET (socket, &set);

        timeout.tv_sec = 1;
        timeout.tv_usec = 5;

        // if server times out
        if(select(FD_SETSIZE, &set, NULL, NULL, &timeout) == 0){
            pthread_mutex_lock(&healthcheck_vals_lock);
            globals->healthcheck_vals[i].requests = INT_MAX;
            globals->healthcheck_vals[i].errors = INT_MAX;
            pthread_mutex_unlock(&healthcheck_vals_lock);
            printf("Server at %d too slow\n", globals->server_ports[i] );
            continue;
        }

        long valread;
        uint8_t request_buffer[150] = {0};
        int offset = 0;

        while(start_of_content((char*) request_buffer) == -1){
            uint8_t buffer[150] = {0};
            valread = read( socket , buffer, 150);  // buffer now holds the first 150 bytes of whatever was sent in the most recent stream
            int i;
            for(i = 0; i<valread; i++){
                request_buffer[offset+i] = buffer[i];
            }
            offset += i;


        }
        int *status_code = malloc(sizeof(int));
        sscanf((char*) request_buffer, "HTTP/1.1 %d", status_code);

        // if wrong status code
        if(*status_code != 200){
            pthread_mutex_lock(&healthcheck_vals_lock);
            globals->healthcheck_vals[i].requests = INT_MAX;
            globals->healthcheck_vals[i].errors = INT_MAX;
            pthread_mutex_unlock(&healthcheck_vals_lock);
            printf("Server at %d wrong status code\n", globals->server_ports[i] );
            continue;
        }

        //request buffer either holds entire content or up to start of content
        int *requests = calloc(1, sizeof(int));
        int *errors = calloc(1, sizeof(int));
        int length = content_length((char*) request_buffer);
        uint8_t* content = request_buffer + start_of_content((char*) request_buffer);
        if(strlen((char*) content) < length){
            uint8_t content[150] = {0};
            valread = read( socket , content, 150);
            sscanf((char*) content, "%d\n%d", errors, requests);
        }else{
            sscanf((char*) content, "%d\n%d", errors, requests);
        }

        // check all the content is numbers or 1 \n char TODO causing failures of test 14 and test 11
        int malformed = 0;
        int newline_count = 0;
        for(int i = 0; i<strlen((char*)content); i++){
            if(content[i] == '\n' && (i == 0 || i == strlen((char*)content) -1)) {
                malformed = 1;
            }else if(content[i] == '\n'){
                newline_count++;
            }else if(!(isdigit(content[i]))){
                malformed = 1;
            }
        }

        if(malformed == 1 || newline_count > 1){
            pthread_mutex_lock(&healthcheck_vals_lock);
            globals->healthcheck_vals[i].requests = INT_MAX;
            globals->healthcheck_vals[i].errors = INT_MAX;
            pthread_mutex_unlock(&healthcheck_vals_lock);
            printf("Server at %d malformed healthcheck\n", globals->server_ports[i] );
            continue;

        }

        printf("req: %d\nerr: %d\n", *requests, *errors);



        pthread_mutex_lock(&healthcheck_vals_lock);
        globals->healthcheck_vals[i].requests = *requests;
        globals->healthcheck_vals[i].errors = *errors;
        pthread_mutex_unlock(&healthcheck_vals_lock);

        free(requests);
        free(errors);



        close(socket);

    }
}

/*
 * sends a healthcheck to all servers every X seconds or R requests
 * storing the healthcheck values in a global array.
 */
void * healthcheck_probe(void* arg){
    Globals *globals = (Globals*) arg;
    probe_server(globals, 1);

    while(1){
        probe_server(globals, 0);
    }
}


int best_server(HealthTuple* healthcheck_vals, int num_servers){
    /*
    return:
    index of min(healthcheck_vals.total_req)
    If there is a tie, return index of min(errors/total_req)
    If there is still a tie, return random index
    */


    int best_server;
    int total_req_counts[num_servers];
    int min = INT_MAX;
    for(int i =0; i<num_servers; i++){
        total_req_counts[i] = healthcheck_vals[i].requests;
        printf("healthcheck_vals[%d] %d\n", i, healthcheck_vals[i].requests);
        if (total_req_counts[i] < min){
            best_server = i;
            min = total_req_counts[i];
        }
    }
    int tie_on_req_counts = -1;
    int contending_servers[num_servers];
    for(int i =0; i<num_servers; i++){
        if(total_req_counts[i] == min){
            tie_on_req_counts++;
            contending_servers[i] = 1;
        }
    }
    int total_req_ratio[num_servers];
    int min_ratio = INT_MAX;
    if(tie_on_req_counts > 0){
        for(int i =0; i<num_servers; i++){
            if(contending_servers[i] == 1){
                float req = healthcheck_vals[i].requests;
                float err = healthcheck_vals[i].errors;
                total_req_ratio[i] = err/req;
                if (total_req_ratio[i] < min_ratio){
                    min_ratio = total_req_ratio[i];
                    best_server = i;
                }
            }
        }
    }

    int tie_on_ratios = -1;
    int contending_servers_2[num_servers];
    for(int i =0; i<num_servers; i++){
        if(total_req_ratio[i] == min_ratio){
            tie_on_ratios++;
            contending_servers_2[i] = 1;
        }

    }

    if(tie_on_ratios > 0 && tie_on_req_counts > 0){
        for(int i =0; i<num_servers; i++){
            if(contending_servers_2[i] == 1){
                return i;
            }
        }
    }


    return best_server;
}


int main(int argc,char **argv) {
    // SETUP OF ARGS AND VALUES
        int client_socket, acceptfd;
        uint16_t connectport, listenport;

        //initialize defaults
        int r_value = 5;
        int num_thread = 4;
        int time_value = 5;


        if (argc < 3) {
            printf("missing arguments: usage %s port_to_connect port_to_listen", argv[0]);
            return 1;
        }

        //parse command line args
        int opt;
        while (1) {
            opt = getopt(argc, argv, "N:R:");
            if (opt == -1){
                break;
            }
            switch (opt) {
                case 'N':
                    num_thread = atoi(optarg);
                    break;
                case 'R':
                    r_value = atoi(optarg);
                    break;
                }
        }

        if(optind != argc){
            listenport = atoi(argv[optind]);
            if(listenport <= 0){
                fprintf(stderr, "%s\n", "YOU MUST GIVE A PORT NUMBER GREATER THAN 0");
                exit(0);
            }
        }

        // parse server ports
        int num_servers = argc-optind-1;
        uint16_t server_ports[num_servers];
        int i = optind;
        i++;
        while(i < argc){
            server_ports[i-optind-1] = atoi(argv[i]);
            if(server_ports[i-optind-1] <= 0){
                fprintf(stderr, "%s\n", "ALL PORT NUMBERS MUST BE GREATER THAN 0");
                exit(0);
            }
            i++;
        }

        //create our thread pool
        pthread_t threads[num_thread];
        for(int i=0; i<num_thread; i++){
            pthread_create(&threads[i], NULL, look_for_work, NULL);
        }


        // Remember to validate return values
        // You can fail tests for not validating


        // open socket to listen for incoming connections to load balancer
        if ((client_socket = server_listen(listenport)) < 0)
            err(1, "failed listening");


        // create healthcheck probing thread shared variables with dispatcher
        // total_req, r_value, time_value, and healthcheck_vals all passed in as thread args
        Globals globals;
        globals.total_req = 0;
        globals.time_value = time_value;
        globals.healthcheck_vals = calloc(num_servers, sizeof(HealthTuple));
        for(int i = 0; i<num_servers; i++){
            globals.healthcheck_vals[i].requests = INT_MAX;
            globals.healthcheck_vals[i].errors = INT_MAX;
        }
        globals.server_ports = server_ports;
        globals.num_servers = num_servers;
        pthread_t healthcheck_probing_thread;
        pthread_create(&healthcheck_probing_thread, NULL, healthcheck_probe, &globals);

        pthread_mutex_lock(&total_req_lock);
        pthread_cond_signal(&total_req_cond_var);
        pthread_mutex_unlock(&total_req_lock);


    // all the loadbalancing work

    while(1){
        struct sockaddr client_addr;
        socklen_t client_addrlen = sizeof(client_addr);
        if ((acceptfd = accept(client_socket, &client_addr, &client_addrlen)) < 0)
            err(1, "failed accepting");
        pthread_mutex_lock(&total_req_lock);
        globals.total_req++;
        if(globals.total_req % r_value == 0){
            pthread_cond_signal(&total_req_cond_var);
        }
        pthread_mutex_unlock(&total_req_lock);


        int *client_socket_pointer = malloc(sizeof(int));
        int *best_server_pointer = malloc(sizeof(int));
        *client_socket_pointer = acceptfd;

        //determine best server to send to
        pthread_mutex_lock(&healthcheck_vals_lock);
        int best_serv = best_server(globals.healthcheck_vals, num_servers);
        printf("best_serv: %d\n", best_serv);
        if ((*best_server_pointer = client_connect(server_ports[best_serv])) < 0){
            printf("All servers down\n");
            char* get_response = "HTTP/1.1 500 INTERNAL SERVER ERROR\r\nContent-Length: 0\r\n\r\n";
            write(acceptfd, get_response, strlen(get_response));
            close(acceptfd);
            pthread_mutex_unlock(&healthcheck_vals_lock);
            continue;
        }

        if(globals.healthcheck_vals[best_serv].requests == INT_MAX){
            char* get_response = "HTTP/1.1 500 INTERNAL SERVER ERROR\r\nContent-Length: 0\r\n\r\n";
            write(acceptfd, get_response, strlen(get_response));
            close(acceptfd);
            pthread_mutex_unlock(&healthcheck_vals_lock);
            continue;

        }

        globals.healthcheck_vals[best_serv].requests++;

        pthread_mutex_unlock(&healthcheck_vals_lock);

        //lock enqueue to ensure no race condidtions
        pthread_mutex_lock(&q_lock);
        enqueue(client_socket_pointer, best_server_pointer);
        pthread_cond_signal(&q_cond_var);
        pthread_mutex_unlock(&q_lock);
    }


}
