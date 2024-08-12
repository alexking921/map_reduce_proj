#include "utils.h"
/*
CSCI 4061 - Project 2 - Final Submission
test machine: csel-kh1250-01.cselabs.umn.edu
date: 03/30/2022

	name: Alex King 
	ID: 3863095
	email: king1493@umn.edu

	name: Crystal Wen
	ID: 438461
	email: wen00015@umn.edu
*/

static int receiveTag = 1000; // for getChunkData() and sendChunkData()
static int receiveTag2 = 2000; //for getInterData() and shuffle()

char *getChunkData(int mapperID) {
  	
	// get key and open msg queue
	key_t key = ftok("src", 10); 
	int msqid = msgget(key, IPC_CREAT|0666);

	// quick error check
	if(msqid == -1){
		perror("ERROR: Cannot get msqid");
		exit(-1);
	}

	struct msgBuffer buf;
	buf.msgType = mapperID;
	int nReadByte = 0;
	
	nReadByte = msgrcv(msqid, &buf, sizeof(buf.msgText), mapperID, 0);

	// quick error check
	if(nReadByte == -1){
		perror("ERROR: Cannot receive message");
		exit(-1);
	}

	/*
		TO-DO: check for END message and send ACK to master and return NULL. Otherwise, 
		return pointer to the chunk data. 
		- once we get to the end of the message, we inform the master by sending our 
		own "ACK" message via msgsnd().
		- the getChunkData() function itself has to return NULL, otherwise mapper will keep 
		calling getChunkData(). Note how we only want to return NULL when we're at the end of the message
	*/ 

	// Checking if the msg we received is the "END" msg...
	if((strcmp(buf.msgText, "END")) == 0){
		// sending "ACK" msg to master
		struct msgBuffer buf2;
		buf2.msgType = receiveTag;
		strcpy(buf2.msgText, "ACK");
		int sendBackToMaster = msgsnd(msqid, &buf2, strlen(buf2.msgText) + 1, buf2.msgType);
		if(sendBackToMaster == -1){
			perror("ERROR: Cannot send message");
			exit(-1);
		}
		return NULL;
	}

	/*
	We're not at the end of the message, we need getChunkData() to actually return the 
	text itself (aka the 'chunkData'):
		Note: In order to return the message (aka buf.msgText) we must can't simply return the pointer because
		the text is on the stack and will be cleared after getChunkData() finishes. So we have to store the
		data on the heap (after allocating memory) then return the corresp pointer.
	*/
	int mSize = sizeof(buf.msgText); // find size of msg for allocating mem on heap
	char *resultText = (char*)malloc(mSize*sizeof(char)); 
	strcpy(resultText, buf.msgText);
	
	return resultText;
} // end of getChunkData()

int getNextWord(int fd, char* buffer){
   char word[100];
   memset(word, '\0', 100);
   int i = 0;
   while(read(fd, &word[i], 1) == 1 ){
    if(word[i] == ' '|| word[i] == '\n' || word[i] == '\t'){
        strcpy(buffer, word);
        return 1;
    }
    if(word[i] == 0x0){
      break;
    }

    i++;
   }
   strcpy(buffer, word);
   return 0;
}

/*
	This function sends chunks of data of size 1024 bytes from inputFile
	in a round-robin fashion to mappers by going through 
	the file one word at a time.
*/

void sendChunkData(char *inputFile, int nMappers) {
	// key_t key;
	// int msgid;
	int fd;
	char buffer[100];
	char chunkBuffer[chunkSize];
	struct msgBuffer msgBuf;
	int mapId = 1;

	chunkBuffer[0] = '\0';
	buffer[0] = '\0';

	// get key and open msg queue
	key_t key = ftok("src", 10);
	int msgid = msgget(key, 0666 | IPC_CREAT);

	// error check
	if(msgid ==-1) {
		perror("ERROR: Fail to get msgid");
		exit(-1);
	}

	// open file for reading...
	fd = open(inputFile, O_RDONLY);
	if(fd == -1) {
		perror("ERROR: file does not exist.");
		exit(-1);
	}

	while(getNextWord(fd, buffer) != 0){
		//sends chunk to the corresponding mapper
		if(strlen(chunkBuffer) + strlen(buffer) > 1024){
			msgBuf.msgType = mapId;
			strcpy(msgBuf.msgText, chunkBuffer);
			msgBuf.msgText[strlen(chunkBuffer)] = '\0';

			int m = msgsnd(msgid, &msgBuf, strlen(msgBuf.msgText) + 1, 0);

			if(m == -1) { // quick err check
				perror("ERROR: Can't send message");
				exit(-1);
			}
			
			++mapId;
			if(mapId > nMappers) 
				mapId = 1;

			memset(chunkBuffer, '\0', chunkSize);
			strcpy(chunkBuffer, buffer);
	
		} else {
			strcat(chunkBuffer, buffer);
			memset(buffer, '\0', 100);
		}
	}

	close(fd);

	// Checks if there are any straggling chunks that are shorter than 1024.
	if(strlen(chunkBuffer) != 0) {
		msgBuf.msgType = mapId;
		strcpy(msgBuf.msgText, chunkBuffer);
		msgBuf.msgText[strlen(chunkBuffer)] = '\0';

		if(msgsnd(msgid, &msgBuf, strlen(msgBuf.msgText) + 1, msgBuf.msgType) == -1) {
			perror("ERROR: Can't send message");
			exit(-1);
		}
	}

  	// inputFile read complete, send END message to mappers
	struct msgBuffer end;
	for(int mapId = 1; mapId <= nMappers; mapId++) {
		end.msgType = mapId;
		strcpy(end.msgText, "END");
		end.msgText[strlen("END")] = '\0';
		if(msgsnd(msgid, &end, strlen(end.msgText) + 1, 0) == -1) {
			perror("ERROR: Can't send END message");
			exit(-1);
		}
	}

  	// wait to receive ACK from all mappers for END notification
	struct msgBuffer msgbuf;
	for(int mapId = 1; mapId <= nMappers; mapId++) {
		if((msgrcv(msgid, &msgbuf, sizeof(msgbuf.msgText), receiveTag, 0)) == -1) {
			perror("ERROR: Can't receive message");
			exit(-1);
		}
	}
	// close the message queue
	msgctl(msgid, IPC_RMID, NULL);
} // end of sendChunkData()



// hash function to divide the list of word.txt files across reducers
//http://www.cse.yorku.ca/~oz/hash.html
int hashFunction(char* key, int reducers){
	unsigned long hash = 0;
    int c;

    while ((c = *key++)!='\0')
        hash = c + (hash << 6) + (hash << 16) - hash;

    return (hash % reducers)+1; // WE HAD TO MANUALLY +1 HERE SO HASH DOESNT RETURN 0
}


// Receives the file path and sends it to reducer
int getInterData(char *key, int reducerID) {
 	// open message queue
	key_t key2;
	key2 = ftok("test", 20); // 20 is hardcoded value we're using

	int msqid = 0;
	msqid = msgget(key2, IPC_CREAT|0666);
	// quick error check
	if(msqid == -1){
		perror("ERROR: Cannot get msqid");
		exit(-1);
	}

  	// receive the file path to word.txt file
  	struct msgBuffer buf;
	buf.msgType = reducerID;
	int nReadByte = 0;
	nReadByte = msgrcv(msqid, &buf, sizeof(buf.msgText), buf.msgType, 0);
	// quick error check
	if(nReadByte == -1){
		perror("ERROR: Cannot receive message getInterData2");
		exit(-1);
	}
	
    // check for END msg and send ACK msg to master, then return 0
    // If not END msg, return 1
	if((strcmp(buf.msgText, "END")) == 0){
		struct msgBuffer buf2;
		buf2.msgType = receiveTag2;
		strcpy(buf2.msgText, "ACK");
		int sendBackToMaster = msgsnd(msqid, &buf2, strlen(buf2.msgText) + 1, buf2.msgType);
		if(sendBackToMaster == -1){
			perror("ERROR: Cannot send message getInterData2");
			exit(-1);
		}
		return 0;
	}

	strcpy(key, buf.msgText);
	return 1;
} // end of getInterData()


// shuffle() uses a HASH function to assign which word.txt files each reducer should 'reduce'. 
void shuffle(int nMappers, int nReducers) {
	// open msg queue
	key_t key2 = ftok("test", 20);
	int msqid = 0;
	msqid = msgget(key2, IPC_CREAT|0666);
	if(msqid == -1){
		perror("ERROR: Cannot get msqid\n");
		exit(-1);
	} 

	char path[1024];
	strcpy(path, "output/MapOut");
	// open the directory "output/MapOut" to access all mapper directories 
	DIR* dir = opendir(path);
	if(dir==NULL){ 
		perror("ERROR: Cannot open directory");
		exit(-1);
	}

	// traverse each of the mapper directories, collecting the filepath of every word.txt file
	struct dirent* entry; // entries in outputMapOut 
	struct dirent* entry2; // entries within a mapper dir (aka the word.txt files assigned to that mapper)
	while((entry = readdir(dir)) != NULL){ 

		// check for the (..) or (.) entry 
		if(!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, "..")){
			continue;
		}
		
		// check if we're at a directory...
		if (entry->d_type == DT_DIR) {
			// building str path to this sub-dir...
			char subDirPath[strlen(path) + strlen(entry->d_name) + 2];
			subDirPath[0] = '\0';
			strcpy(subDirPath, path);
			strcat(subDirPath, "/");
			strcat(subDirPath, entry->d_name); // 'subDirPath' is now similar to: "output/MapOut/Map_1"

			// open and search this DIR (which will be all .txt files, no other dir type entries)
			DIR* dir2 = opendir(subDirPath);
			if(dir2==NULL){ // quick error check
				perror("ERROR: Cannot open directory");
				exit(-1);
			}

			// visit every entry...
			while((entry2 = readdir(dir2)) != NULL){ 
				// check for the (..) or (.) entry 
				if(!strcmp(entry2->d_name, ".") || !strcmp(entry2->d_name, "..")){
					continue; 
				}
				// check if entry is reg file (.txt file)
				if (entry2->d_type == DT_REG) {      
					// 1a. get the NAME of the file
					char txtFileName[strlen(entry2->d_name)+ 1]; 
					strcpy(txtFileName, entry2->d_name);
					
					// 1b. build PATH to the file
					char pathToTxtFile[strlen(path) + (strlen(entry->d_name)) + strlen(entry2->d_name) + 2];
					pathToTxtFile[0] = '\0';
					strcat(pathToTxtFile, path);
					strcat(pathToTxtFile, "/");
					strcat(pathToTxtFile, entry->d_name);
					strcat(pathToTxtFile, "/");
					strcat(pathToTxtFile, entry2->d_name);

					// 2. Call hashFunction() to get the reducerID assigned to the .txt file
					int reducerChosenByHash = hashFunction(txtFileName, nReducers);

					// 3. using msgsnd(), send the PathToTxtFile to reducerID (using hashFunction return) 
					struct msgBuffer mBuf4;
					mBuf4.msgType = reducerChosenByHash; 
					strcpy(mBuf4.msgText, pathToTxtFile);

					int sendTxtPathToReducer = msgsnd(msqid, &mBuf4, strlen(mBuf4.msgText)+1, mBuf4.msgType); 
					if(sendTxtPathToReducer == -1){
						perror("ERROR: Cannot send message (MSG from Shuffle() FAILED TO SEND TO THE REDUCER)");
						exit(-1);
					}

					memset(pathToTxtFile, '\0', sizeof(pathToTxtFile));

				} 
			}

			closedir(dir2); // close inner directory
		}
  	}

  	closedir(dir); // close outer directory

	// send END message to reducers (now that read of inputFile is finished) 
	for(int i = 1; i<= nReducers; i++){ 
		struct msgBuffer mBufEnd;
		mBufEnd.msgType = i; 
		strcpy(mBufEnd.msgText, "END");
		int endMsgForReducers = msgsnd(msqid, &mBufEnd, strlen(mBufEnd.msgText)+1, 0);
		if(endMsgForReducers == -1){
			perror("ERROR: Cannot send message (AFTER nested loops in shuffle)");
			exit(-1);
		}
	}

	// wait for ACK from the reducers, then send END msg to getInterData()
	struct msgBuffer ackRcvBuf;
	for(int mapId = 1; mapId <= nReducers; mapId++) {
		if((msgrcv(msqid, &ackRcvBuf, sizeof(ackRcvBuf.msgText), receiveTag2, 0)) == -1) {
			perror("ERROR: Can't receive message");
			exit(-1);
		}
	}
	// close message queue
	msgctl(msqid, IPC_RMID, NULL);
} // end of shuffle()



// check if the character is valid for a word
int validChar(char c){
	return (tolower(c) >= 'a' && tolower(c) <='z') ||
					(c >= '0' && c <= '9');
}

char *getWord(char *chunk, int *i){
	char *buffer = (char *)malloc(sizeof(char) * chunkSize);
	memset(buffer, '\0', chunkSize);
	int j = 0;
	while((*i) < strlen(chunk)) {
		if (chunk[(*i)] == '\n' || chunk[(*i)] == ' ' || !validChar(chunk[(*i)]) || chunk[(*i)] == 0x0) {
			buffer[j] = '\0';
			if(strlen(buffer) > 0){
				(*i)++;
				return buffer;
			}
			j = 0;
			(*i)++;
			continue;
		}
		buffer[j] = chunk[(*i)];
		j++;
		(*i)++;
	}
	if(strlen(buffer) > 0)
		return buffer;
	return NULL;
}

void createOutputDir(){
	mkdir("output", ACCESSPERMS);
	mkdir("output/MapOut", ACCESSPERMS);
	mkdir("output/ReduceOut", ACCESSPERMS);
}

char *createMapDir(int mapperID){
	char *dirName = (char *) malloc(sizeof(char) * 100);
	memset(dirName, '\0', 100);
	sprintf(dirName, "output/MapOut/Map_%d", mapperID);
	mkdir(dirName, ACCESSPERMS);
	return dirName;
}

void removeOutputDir(){
	pid_t pid = fork();
	if(pid == 0){
		char *argv[] = {"rm", "-rf", "output", NULL};
		if (execvp(*argv, argv) < 0) {
			printf("ERROR: exec failed\n");
			exit(1);
		}
		exit(0);
	} else{
		wait(NULL);
	}
}

void bookeepingCode(){
	removeOutputDir();
	sleep(1);
	createOutputDir();
}



/*
 CUSTOM TESTS:

	mapreduce 5 2 test/T1/F2.txt
 	F2.txt
		the 90
		man 30
		walked 30
		over 30
		hill 30
		to 30
		very 30
		top 30
		hillToHill 1
		manyTimeHe 2

	mapreduce 5 2 test/T1/F3.txt
	F3.txt
		minnesota 20
		minneapolis 10
		university 10
		twenty 10
		or 10
		a 30

*/