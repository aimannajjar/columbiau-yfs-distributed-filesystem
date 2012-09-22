// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

#ifdef DEBUG
# define DEBUG_PRINT(x) printf x
#else
# define DEBUG_PRINT(x) do {} while (0)
#endif


pthread_mutex_t lock_server::ltable_m = PTHREAD_MUTEX_INITIALIZER;

lock_server::lock_server():
  nacquire (0)
{

}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  DEBUG_PRINT(("stat request from clt %d\n", clt));
  r = nacquire;
  

  return ret;
}


lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{

	DEBUG_PRINT(("Client %d is attempting to acquire lock %llu\n", clt, lid));

	// default, assume everything will be ok
	lock_protocol::status ret = lock_protocol::OK;
	r = 0;
	

	// requesting access to shared lock_table
	if (pthread_mutex_lock(&ltable_m) < 0)
	{
		// mutex error, return to client with -1 error code
		serverWarning("pthread_mutex_lock");
		DEBUG_PRINT(("Error occurred while acquiring lock %llu\n", lid));
		r = -1;		
		return ret;
	}

	// new lock?
	struct remote_lock* rl;
	if (locks_table.count(lid) == 0)
	{
		// yes, do some allocations 
		rl = (struct remote_lock*)malloc(sizeof(struct remote_lock));
		pthread_mutex_t* mutex = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
		pthread_cond_t* cond = (pthread_cond_t*)malloc(sizeof(pthread_cond_t));

		// error checking
		if (rl == NULL || mutex == NULL )
		{
			serverWarning("malloc");
			DEBUG_PRINT(("Error occurred while acquiring lock %llu\n", lid));
			r = -2;
		}
		else if (pthread_mutex_init(mutex, NULL) != 0)
		{
			serverWarning("pthread_mutex_init");
			DEBUG_PRINT(("Error occurred while acquiring lock %llu\n", lid));
			r = -3;
		}
		else if (pthread_cond_init(cond, NULL) != 0)
		{
			pthread_mutex_destroy(mutex);
			serverWarning("pthread_cond_init");
			DEBUG_PRINT(("Error occurred while acquiring lock %llu\n", lid));
			r = -4;			
		}
		else
		{
			// initilaize lock struct and insert into locks table, initally lock is free and not owned yet
			rl->lock_state = remote_lock::FREE;
			rl->mutex = mutex;
			rl->cond = cond;
			rl->requests = 1;
			locks_table[lid] = rl;
		}
	}
	else
	{
		rl = locks_table[lid]; // existing lock
		rl->requests++;
	}

	// we don't need locks table anymore, release lock
	if (pthread_mutex_unlock(&ltable_m) != 0)
		serverFatalError("pthread_mutex_unlock"); // this should never happen...	


	// acquire mutex and change lock ownership & state IF lock is available, otherwise wait
	if (pthread_mutex_lock(rl->mutex) != 0)
	{
		// failed!
		serverWarning("pthread_mutex_lock");
		DEBUG_PRINT(("Error occurred while acquiring lock %llu\n", lid));
		r = -5;				
	}
	else
	{
		// ----------------------------------------------
		// start of critical section for lock acquisition
		// ----------------------------------------------
		if (rl->lock_state != remote_lock::FREE)
		{
			// lock is taken, wait for it to free
			while (rl->lock_state != remote_lock::FREE) 
			{
				if (pthread_cond_wait(rl->cond, rl->mutex) < 0)
				{
					r = -6;
					break;
				}
			}
			// lock has been released (or pthread_cond_wait failed)
		}

		if (r == 0)
		{
			// Lock Acquired!
			rl->lock_state = remote_lock::LOCKED;
			rl->current_owner = clt;	
			rl->requests--;

			DEBUG_PRINT(("Client %d has acquired lock %llu\n", clt, lid));
		}
		
		// ----------------------------------------------
		// end of critical section for lock acquisition
		// ----------------------------------------------
		if (pthread_mutex_unlock(rl->mutex) != 0)
			serverFatalError("pthread_mutex_unlock"); // this should never happen...	

	}

	DEBUG_PRINT(("Client %d acquire of lock %llu returned with code %d\n", clt, lid, r));

	return ret;
}





lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
	DEBUG_PRINT(("Client %d is attempting to release lock %llu\n", clt, lid));

	// default, assume everything will be ok
	lock_protocol::status ret = lock_protocol::OK;
	r = 0;
	bool lock_destroyed = false; // flag indicates whether this release resulted in this lock being removed from our table

	// requesting access to shared lock_table
	if (pthread_mutex_lock(&ltable_m) != 0)
	{
		// mutex error, return to client with -1 error code
		serverWarning("pthread_mutex_lock");
		DEBUG_PRINT(("Error occurred while releasing lock %llu\n", lid));
		r = -1;		
		return ret;
	}

	

	// find lock
	struct remote_lock* rl = NULL;
	if (locks_table.count(lid) != 0)		
		rl = locks_table[lid]; 

	// relinquish access to locks table
	if (pthread_mutex_unlock(&ltable_m) != 0)
		serverFatalError("pthread_mutex_unlock"); // this should never happen...	

	if (rl == NULL)
		r = -2;
	// request access to lock's struct and change lock state & signal other threads IF lock is actually mine, otherwise return with error
	else if (pthread_mutex_lock(rl->mutex) != 0)
	{
		// failed!
		serverWarning("pthread_mutex_lock");
		DEBUG_PRINT(("Error occurred while releasing lock %llu\n", lid));
		r = -3;				
	}
	else
	{
		// ----------------------------------------------
		// start of critical section for lock release
		// ----------------------------------------------
		if (rl->lock_state != remote_lock::LOCKED || rl->current_owner != clt)
			// this is lock is either not mine, or it isn't even currently locked...
			r = -4;
		else
		{
			// So far good, change lock data
			rl->lock_state = remote_lock::FREE;
			rl->current_owner = 0;	
			
			DEBUG_PRINT(("Client %d has released lock %llu\n", clt, lid));

			// If there aren't any requests waiting for this lock, let's destroy this lock to free up some memory
			if (rl->requests <= 0)
			{
				// to guarantee thread-safety, "requests" is incremented with the protection of the global ltable_m
				// so we need quick access to ltable_m before we are 100% positive we can destroy the lock
				if (pthread_mutex_lock(&ltable_m) == 0) // this could cause performance bottleneck ?
				{
					if (rl->requests <= 0) // check again, a request may have just been made
					{
						// it's safe to unlock lock's mutex here, since there are no requests
						// are waiting in the pipeline anyway for this lock anyway
						if (pthread_mutex_unlock(rl->mutex) != 0)
							serverFatalError("pthread_mutex_unlock"); // this should never happen...	

						pthread_mutex_destroy(rl->mutex);
						pthread_cond_destroy(rl->cond);
						free(rl->mutex);
						free(rl->cond);
						free(rl);
						locks_table.erase(lid);
						lock_destroyed = true;

					}
					pthread_mutex_unlock(&ltable_m);
				}
			}
		
		}
		// ----------------------------------------------
		// end of critical section for lock release
		// ----------------------------------------------
		if (!lock_destroyed && pthread_mutex_unlock(rl->mutex) != 0)
			serverFatalError("pthread_mutex_unlock"); // this should never happen...	

	}

	if (!lock_destroyed)
		pthread_cond_signal(rl->cond);
	else
		DEBUG_PRINT(("Lock %llu destroyed\n", lid));

	DEBUG_PRINT(("Client %d release of lock %llu returned with code %d\n", clt, lid, r));


	return ret;
}