#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/shm.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <fcntl.h>
#include <sys/msg.h>

#define PORT 5679
#define BUFFERSIZE 1024
#define SEGMENTSIZE sizeof(keyValuePair)


typedef struct keyValuePair {
    char key[BUFFERSIZE];
    char value[BUFFERSIZE];
    int suscribers[1];
} keyValuePair;

struct textMessage {
    long mtype;
    char mMessage[100];
};



int selectCommand(char* command);
int putKeyValue(keyValuePair* keyValueStore, char* key, char* value);
int getKeyValue(keyValuePair* keyValueStore, char* key);
int delKeyValue(keyValuePair* keyValueStore, char* key);
int checkForSuscribers(keyValuePair* keyValueStore, char* key);



int main(){

    //SERVERT SOCKET
    int socketFD, ret, bytes_read, n;
    struct sockaddr_in serverAddr;

    int newSocket;
    struct sockaddr_in newAddr;

    socklen_t addr_size;

    char buffer[BUFFERSIZE];
    char workingBuffer[BUFFERSIZE];
    char outputBuffer[BUFFERSIZE];
    pid_t childpid;

    socketFD = socket(AF_INET, SOCK_STREAM, 0);
    if(socketFD < 0){
        printf("[-]Error in connection.\n");
        exit(1);
    }
    printf("[+]Server Socket is created.\n");

    int option = 1;
    if (setsockopt(socketFD, SOL_SOCKET, SO_REUSEADDR, (char *)&option, sizeof(option)) == -1 ) {  //Für schnelles binden
        printf("[-]Error while setting socket options\n");
        return -2;
    }

    memset(&serverAddr, '\0', sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(PORT);
    serverAddr.sin_addr.s_addr = inet_addr("127.0.0.1");

    ret = bind(socketFD, (struct sockaddr*)&serverAddr, sizeof(serverAddr));
    if(ret < 0){
        printf("[-]Error in binding.\n");
        exit(1);
    }
    printf("[+]Bind to port %d\n", 4444);

    if(listen(socketFD, 10) == -1){
        printf("[-]Error in Listening\n");
    }else{
        printf("[+]Listening....\n");
    }

    //SHARED MEMORY
    int sharedMemorySegmentID;
    keyValuePair* sharedMemorySegment;

    sharedMemorySegmentID = shmget(IPC_PRIVATE, 10*SEGMENTSIZE, IPC_CREAT | 0600);
    if (sharedMemorySegmentID == -1) {
        perror("[-]Error while getting shared memory segment\n");
        return -1;
    }

    sharedMemorySegment = (keyValuePair*) shmat(sharedMemorySegmentID, 0, 0);
    if (sharedMemorySegment == (void*) -1) {
        perror("[-]Error while attaching shared memory segment\n");
        return -2;
    }

    if ((n = shmctl(sharedMemorySegmentID, IPC_RMID, 0)) == -1) {
        perror("[-]Error while destroying shared memory segment\n");
        return -3;
    }

    for (int i = 0; i < 3; i++) {
        //evt. verbesserung keyValuePair* hilfszeiger = sharedMemorySegment + i*sizeof(keyValuePair);
        strcpy(sharedMemorySegment->key + i*sizeof(keyValuePair), "0");
        strcpy(sharedMemorySegment->value + i*sizeof(keyValuePair), "0");
        memset(sharedMemorySegment->suscribers + i*sizeof(keyValuePair), 0, sizeof(int));
        printf("Subscribers: [%d], %d\n", *(sharedMemorySegment->suscribers + i*sizeof(keyValuePair)), i); //HILFSAUSGABE
    }
    printf("GOT shared Memory\n");

    //SEMAPHORE
    int sem_id;
    sem_id = semget (IPC_PRIVATE, 1, IPC_CREAT|0644);
    if (sem_id == -1) {
        perror ("Die Gruppe konnte nicht angelegt werden!");
        exit(1);
    }

    // Anschließend wird der Semaphor auf 1 gesetzt

    semctl(sem_id, 1, SETVAL, 0);  // alle Semaphore auf 1

    unsigned short test[1];
    semctl(sem_id, 1, GETALL, test);
    printf("Test: %hu\n", test[0]);

    /*
     If sem_op is zero, the process must have read permission on the
       semaphore set.  This is a "wait-for-zero" operation: if semval is
       zero, the operation can immediately proceed.
    */

    /*
    Erklärung:
    Semaphor wird mit 0 initialisiert
    Jeder Prozess betritt den kritischen Bereich nur, wenn das Semaphor 0 beträgt und
    eine interne TransaktionRunning false (0) ist
    bei dem Semaphor wird aber nichts an seinem Wert verändert
    Bei dem BEG einer Transaktion wird der Semaphor auf 1 gesetzt
    und die interne Variable TransktionRunning auf true (1) gesetzt,
    weshalb nicht mehr ein Semaphor von 0 die Vorraussetzung für den weiteren Prozessablauf ist
    */


    struct sembuf wait;
    wait.sem_num = 0; //Semaphor 0 in der Gruppe
    wait.sem_flg = SEM_UNDO;
    wait.sem_op = 0; //DOWN

    struct sembuf enter = {.sem_num = 0, .sem_op = 1, .sem_flg = SEM_UNDO};
    struct sembuf leave = {.sem_num = 0, .sem_op = -1, .sem_flg = SEM_UNDO};


    /*
    Ergänzung des KeyValuePairs um einen int[] mit den socket_FD der abonnenten
    MessageQueue erstellen
    wenn ein Wert verändert wird (PUT/DEL) -> Überprüfung ob es Abonnenten gibt
    wenn ja, Nachricht an alle Prozesse mit dem Index
    Abfrage der eigenen Socket_FD und vergleich mit der Abonnenten
    */

    //MESSAGE QUEUE || SUB / PUB
    int messageQueueID;
    struct textMessage message;


    messageQueueID = msgget(IPC_PRIVATE, IPC_CREAT | 0644);
    if (messageQueueID == -1) {
        perror("msgget()");
        exit(EXIT_FAILURE);
    }

    message.mtype = 1;

    int subsRunning = 0;
    int indexOfSubscribdedKeys[10];
    int socketOfSubscribedKey = 0;
    int indexOfSuscriber = 0;



    //COMMUNICATION PART
    int commandSelected;
    char* commandPart;
    char* keyPart;
    char* valuePart;

    //TRANSACTIONS
    int transactionRunning = 0;

    //SERVER PROCESS
    int running = 1;
    while(running == 1){
        newSocket = accept(socketFD, (struct sockaddr*)&newAddr, &addr_size);
        if(newSocket == -1){
            exit(1);
        }

        printf("Connection accepted from %s:%d\n", inet_ntoa(newAddr.sin_addr), ntohs(newAddr.sin_port));

        if((childpid = fork()) == 0){
            close(socketFD);

            while(1){

                if (subsRunning == 1) {
                    if (msgrcv(messageQueueID, &message, sizeof(message), (long) newSocket, 0) == -1) {
                        perror("msgrcv()");
                        printf("Error while recieving message\n");
                        exit(EXIT_FAILURE);
                    }
                    else {
                        printf("Message received!\n");
                        printf("Message: %s, message_typ: %ld\n", message.mMessage, message.mtype);
                        if ((n = getKeyValue(sharedMemorySegment, message.mMessage)) == -1) {
                            printf("SUB key was deleted\n");
                            strcpy(outputBuffer, "DEL:");
                            strcat(outputBuffer, message.mMessage);
                            strcat(outputBuffer, ":");
                            strcat(outputBuffer, "key_deleted");
                        }
                        else {
                            printf("%s changed to %s\n", sharedMemorySegment->key + n * sizeof(keyValuePair), sharedMemorySegment->value + n*sizeof(keyValuePair));
                            printf("Index of changed value is %d, new Value is %s\n", n, sharedMemorySegment->value + n*sizeof(keyValuePair));

                            if (strcmp(sharedMemorySegment->value +n*sizeof(keyValuePair), "0") != 0) {
                                printf("SUB Key value was changed\n");
                                strcpy(outputBuffer, "PUT:");
                                strcat(outputBuffer, sharedMemorySegment->key + n*sizeof(keyValuePair));
                                strcat(outputBuffer, ":");
                                strcat(outputBuffer, sharedMemorySegment->value + n*sizeof(keyValuePair));
                            }
                        }
                        send(newSocket, outputBuffer, sizeof(outputBuffer), 0);
                        printf("Sending to subbed client: [%s]\n", outputBuffer);
                        memset(outputBuffer, 0, sizeof(outputBuffer));

                    }
                }
                if((bytes_read = recv(newSocket, buffer, 1024, 0)) > 0) {
                    buffer[bytes_read] = '\0';


                    if(strcmp(buffer, "QUIT") == 0){
                        printf("Disconnected from %s:%d\n", inet_ntoa(newAddr.sin_addr), ntohs(newAddr.sin_port));
                        break;
                    }
                    else{

                        if (transactionRunning == 0) {
                            printf("Before wait\n");
                            semop(sem_id, &wait, 1);
                            printf("After wait\n");
                        }
                        else
                        {
                            printf("Transaction running so no need to wait\n");
                        }

                        strcpy(workingBuffer, buffer);
                        printf("Working: [%s]\n", workingBuffer);

                        commandPart = strtok(workingBuffer, " ");
                        commandSelected = selectCommand(commandPart);

                        switch (commandSelected)
                        {
                            case 0:
                                break;

                            case 1:
                                //PUT
                                printf("Client: [%s]\n", buffer);
                                keyPart = strtok(NULL, " ");
                                valuePart = strtok(NULL, "\0");
                                printf("PUT [%s] [%s]\n", keyPart, valuePart);
                                if ((n = putKeyValue(sharedMemorySegment, keyPart, valuePart)) == -1) {
                                    printf("Couldnt PUT KEY VAL\n");
                                    strcpy(outputBuffer, "PUT:");
                                    strcat(outputBuffer, keyPart);
                                    strcat(outputBuffer, ":");
                                    strcat(outputBuffer, valuePart);
                                    strcat(outputBuffer, ":not_possible");
                                }
                                else {
                                    printf("PUT [%s] [%s]\n", keyPart, valuePart);
                                    strcpy(outputBuffer, "PUT:");
                                    strcat(outputBuffer, keyPart);
                                    strcat(outputBuffer, ":");
                                    strcat(outputBuffer, valuePart);

                                    // SUB CHECK
                                    indexOfSuscriber = checkForSuscribers(sharedMemorySegment, keyPart);
                                    if (indexOfSuscriber == -1) {
                                        printf("No suscriber found\n");
                                    }
                                    else if (indexOfSuscriber >= 0) {
                                        printf("Subscriber found at index %d\n", indexOfSuscriber);
                                        strcpy(message.mMessage, sharedMemorySegment->key + indexOfSuscriber*sizeof(keyValuePair));
                                        message.mtype = (long) *(sharedMemorySegment->suscribers + indexOfSuscriber*sizeof(keyValuePair));
                                        printf("Message typ is %ld and key is %s\n", message.mtype, message.mMessage);
                                        if (msgsnd(messageQueueID, &message, sizeof(message), 0) == -1) {
                                            perror("msgsnd()");
                                            exit(EXIT_FAILURE);
                                        }
                                        printf("Message send!\n");
                                    }
                                }

                                break;

                            case 2:
                                printf("Client: [%s]\n", buffer);

                                keyPart = strtok(NULL, "\0");
                                printf("GET [%s]\n", keyPart);
                                if ((n = getKeyValue(sharedMemorySegment, keyPart)) == -1) {
                                    printf("KEY_NOT_FOUND\n");
                                    strcpy(outputBuffer, "GET:");
                                    strcat(outputBuffer, keyPart);
                                    strcat(outputBuffer, ":key_nonexistent");
                                }
                                else {
                                    printf("GET:%s:%s\n", keyPart, sharedMemorySegment->value + n*sizeof(keyValuePair));
                                    strcpy(outputBuffer, "GET:");
                                    strcat(outputBuffer, keyPart);
                                    strcat(outputBuffer, ":");
                                    strcat(outputBuffer, sharedMemorySegment->value + n*sizeof(keyValuePair));

                                }

                                break;

                            case 3:
                                printf("Client: [%s]\n", buffer);

                                keyPart = strtok(NULL, "\0");
                                printf("DEL [%s]\n", keyPart);

                                // SUB CHECK
                                indexOfSuscriber = checkForSuscribers(sharedMemorySegment, keyPart);
                                if (indexOfSuscriber == -1) {
                                    printf("No suscriber or no key found\n");
                                }
                                else if (indexOfSuscriber >= 0) {
                                    printf("Subscriber found at index %d\n", indexOfSuscriber);
                                    strcpy(message.mMessage, sharedMemorySegment->key + indexOfSuscriber*sizeof(keyValuePair));
                                    message.mtype = (long) *(sharedMemorySegment->suscribers + indexOfSuscriber*sizeof(keyValuePair));
                                    printf("Message typ is %ld and key is %s\n", message.mtype, message.mMessage);
                                    if (msgsnd(messageQueueID, &message, sizeof(message), 0) == -1) {
                                        perror("msgsnd()");
                                        exit(EXIT_FAILURE);
                                    }
                                    printf("Message send!\n");
                                }

                                if ((n = delKeyValue(sharedMemorySegment, keyPart)) == -1) {
                                    printf("KEY_NOT_FOUND\n");
                                    strcpy(outputBuffer, "DEL:");
                                    strcat(outputBuffer, keyPart);
                                    strcat(outputBuffer, ":key_nonexistent");
                                }
                                else {
                                    printf("KEY_DELETED\n");
                                    strcpy(outputBuffer, "DEL:");
                                    strcat(outputBuffer, keyPart);
                                    strcat(outputBuffer, ":key_deleted");


                                }

                                break;

                            case 4:
                                running = 0;
                                goto Q;
                                break;

                            case 5:
                                strcpy(outputBuffer, "PUT <key> <value>\n\t\tGET <key>\n\t\tDEL <key>\n\t\tBEG\n\t\tEND\n\t\tSUB\n\t\tQUIT");
                                break;

                            case 6:
                                printf("BEG Transaction\n");
                                semctl(sem_id, 1, GETALL, test);
                                printf("Test before BEG: %hu\n", test[0]);
                                semop(sem_id, &enter, 1);
                                semctl(sem_id, 1, GETALL, test);
                                printf("Test after BEG: %hu\n", test[0]);
                                strcpy(outputBuffer, "BEG:TRANSACTION");
                                transactionRunning = 1;
                                printf("set transation to running %d\n", transactionRunning);
                                break;

                            case 7:
                                printf("END Transaction\n");
                                semctl(sem_id, 1, GETALL, test);
                                printf("Test before END: %hu\n", test[0]);
                                semop(sem_id, &leave, 1);
                                semctl(sem_id, 1, GETALL, test);
                                printf("Test after END: %hu\n", test[0]);
                                strcpy(outputBuffer, "END:TRANSACTION");
                                transactionRunning = 0;
                                printf("Stop transaction running %d\n", transactionRunning);
                                break;

                            case 8:
                                keyPart = strtok(NULL, "\0");
                                printf("SUB [%s]\n", keyPart);
                                if ((n = getKeyValue(sharedMemorySegment, keyPart)) == -1) {
                                    printf("KEY_NOT_FOUND\n");
                                    strcpy(outputBuffer, "SUB:");
                                    strcat(outputBuffer, keyPart);
                                    strcat(outputBuffer, ":key_nonexistent");
                                }
                                else {
                                    printf("GET to SUB:%s:%s\n", keyPart, sharedMemorySegment->value + n*sizeof(keyValuePair));
                                    strcpy(outputBuffer, "SUB:");
                                    strcat(outputBuffer, keyPart);
                                    strcat(outputBuffer, ":");
                                    strcat(outputBuffer, sharedMemorySegment->value + n*sizeof(keyValuePair));

                                    //memset(socketValue, '\0', sizeof(socketValue));
                                    memcpy(sharedMemorySegment->suscribers + n*sizeof(keyValuePair), &newSocket, sizeof(int));
                                    printf("Added socket %d to suscribers %d\n", newSocket, *(sharedMemorySegment->suscribers + n*sizeof(keyValuePair)));
                                    subsRunning++;
                                    printf("Added running sub: %d\n", subsRunning);

                                }
                                break;

                            case 9:
                                keyPart = strtok(NULL, "\0");
                                printf("UNSUBSCRIBE [%s]\n", keyPart);
                                if ((n = getKeyValue(sharedMemorySegment, keyPart)) == -1) {
                                    printf("KEY_NOT_FOUND\n");
                                    strcpy(outputBuffer, "UNSUB:");
                                    strcat(outputBuffer, keyPart);
                                    strcat(outputBuffer, ":key_nonexistent");
                                }
                                else {
                                    printf("GET to UNSUB:%s:%s\n", keyPart, sharedMemorySegment->value + n*sizeof(keyValuePair));
                                    strcpy(outputBuffer, "UNSUB:");
                                    strcat(outputBuffer, keyPart);
                                    memset(sharedMemorySegment->suscribers + n*sizeof(keyValuePair), 0, sizeof(int));
                                    printf("Deleted socket %d to unsub %d\n", newSocket, *(sharedMemorySegment->suscribers + n*sizeof(keyValuePair)));
                                    subsRunning--;
                                    printf("Decrementet running sub: %d\n", subsRunning);

                                }
                                break;

                            default:
                                strcpy(outputBuffer, commandPart);
                                strcat(outputBuffer, ":command_nonexistent");
                                break;
                        }
                        //////////////////////////////////////
                        printf("SEND: [%s]\n", outputBuffer);
                        send(newSocket, outputBuffer, sizeof(buffer), 0);
                        //bzero(buffer, sizeof(buffer));
                        memset(buffer, '\0', sizeof(buffer));
                        memset(workingBuffer, '\0', sizeof(workingBuffer));
                        memset(outputBuffer, '\0', sizeof(outputBuffer));
                        printf("Buffer: [%s]\n", buffer);



                    }
                }
            }}

        else if (childpid == -1) {
            perror("[-]Error while creating child process");
            semctl(sem_id, 0, IPC_RMID);
            exit(EXIT_FAILURE);
        }

    }

    Q:	close(newSocket);

    if ((n= shmdt(sharedMemorySegment)) == -1) {
        perror("[-]Error while deteching shared memory segment");
        return -15;
    }
    printf("[+]Shared memory segment detached\n");


    if(semctl(sem_id, 0, IPC_RMID) == -1) {
        perror("[-]Error while semctl()");
    }
    printf("[+]Semaphor destroyed\n");

    if (msgctl(messageQueueID, IPC_RMID, NULL) == -1) {
        perror("[-]Error while msgctl");
        exit(EXIT_FAILURE);
    }
    printf("[+]Message queue destroyed\n");


    return 0;
}

int selectCommand(char* commandPart) {
    printf("selectCommand() for [%s]\n", commandPart);

    if (strcmp(commandPart, "QUIT") == 0) {
        printf("Command: QUIT\n");
        return 0;
    }

    else if (strcmp(commandPart, "PUT") == 0) {
        printf("Command: PUT\n");
        return 1;
    }

    else if (strcmp(commandPart, "GET") == 0) {
        printf("Command: GET\n");
        return 2;
    }

    else if (strcmp(commandPart, "DEL") == 0) {
        printf("Command: DEL\n");
        return 3;
    }

    else if (strcmp(commandPart, "STOP") == 0) {
        printf("Command: STOP\n");
        return 4;
    }

    else if (strcmp(commandPart, "HELP") == 0) {
        printf("Command: HELP\n");
        return 5;
    }

    else if (strcmp(commandPart, "BEG") == 0) {
        return 6;
    }

    else if (strcmp(commandPart, "END") == 0) {
        return 7;
    }

    else if (strcmp(commandPart, "SUB") == 0) {
        return 8;
    }

    else if (strcmp(commandPart, "UNSUB") == 0) {
        return 9;
    }

    else {
        printf("Command unknown\n");
        return -1;
    }
}


int putKeyValue(keyValuePair* keyValueStore, char* key, char* value) {
    for (int i = 0; i < 10; i++) {
        if (strcmp(keyValueStore->key + i*sizeof(keyValuePair), key) == 0 || strcmp(keyValueStore->key + i*sizeof(keyValuePair), "0") == 0) {
            strcpy(keyValueStore->key + i*sizeof(keyValuePair), key);
            strcpy(keyValueStore->value + i*sizeof(keyValuePair), value);
            return i;
        }
    }
    return -1;
}

//RETURNS INDEX OF KEY
int getKeyValue(keyValuePair* keyValueStore, char* key) {
    for (int i = 0; i < 10; i++) {
        if (strcmp(keyValueStore->key + i*sizeof(keyValuePair), key) == 0) {
            return i;
        }
    }
    return -1;
}

//EVT. EINBINDUNG VON getKeyValue()
int delKeyValue(keyValuePair* keyValueStore, char* key) {
   keyValuePair* runPointer = keyValueStore;
    for (int i = 0; i < 10; i++) {
        if (strcmp(keyValueStore->key + i*sizeof(keyValuePair), key) == 0) {
            strcpy(keyValueStore->key + i*sizeof(keyValuePair), "0");
            strcpy(keyValueStore->value + i*sizeof(keyValuePair), "0");
            *runPointer->suscribers = 0;
            return 0;
        }
        runPointer++;
    }
    return -1;
}

int checkForSuscribers(keyValuePair* keyValueStore, char* key) {
    printf("Checking for suscribers\n");

    int index;

    if ((index = getKeyValue(keyValueStore, key)) == -1) {
        printf("No key found so no subs\n");
        return -1;
    }

    printf("Key found at Index %d\n", index);

    if (*(keyValueStore->suscribers + index*sizeof(keyValuePair)) > 0) {
        printf("Suscriber found %d\n", *(keyValueStore->suscribers + index*sizeof(keyValuePair)));
        return index;
    }

    printf("No suscribers found\n");
    return -1;
}


/*
message_type auf den suscriber socket setzen
jeder prozess nimmt nur seinen socket als message_type
reaction
*/