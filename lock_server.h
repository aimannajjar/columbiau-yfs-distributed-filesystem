// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include <stdio.h>
#include <stdlib.h>
#include "common.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock_server {
private:
	static pthread_mutex_t ltable_m;


protected:
	int nacquire;

public:
 	
	std::map<lock_protocol::lockid_t,struct remote_lock*> locks_table;

 	lock_server();
 	~lock_server() {};
 	inline void serverFatalError(const char* call) { 
 		perror(call); 
 		fprintf(stderr, "Server thread %d exited because of previous error\n", getpid()); 
 		exit(-1); 
 	};

 	inline void serverWarning(const char* call) { 
 		perror(call); 
 		fprintf(stderr, "Warning: unexpeced error occurred in server thread %d \n", getpid()); 
 	};

 	lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
 	lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
 	lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);

};


struct remote_lock
{
	enum lock_state { FREE, LOCKED };
	int lock_state;
	pthread_mutex_t* mutex;	
	pthread_cond_t* cond;
	int current_owner;
	int requests; /* number of clients waiting for this lock */
};



#endif 







