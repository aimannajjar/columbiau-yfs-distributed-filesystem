// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "lock_server_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>



static void
free_lock(struct lock_struct* lock)
{
  pthread_mutex_destroy(lock->mutex);
  pthread_cond_destroy(lock->cond);
  free(lock->mutex);
  free(lock->cond);
  free(lock);

}


static void *
releasethread(void *x)
{
  lock_client_cache *cc = (lock_client_cache *) x;
  cc->releaser();
  return 0;
}

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  :  lu(_lu) 
{
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // assert(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs* rlsrpc = new rpcs(rlock_port);



  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke);

  assert(pthread_cond_init(&releaser_cond, NULL) == 0);
  assert(pthread_mutex_init(&releaser_m, 0) == 0);

  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  assert (r == 0);

  sockaddr_in dstsock;
  make_sockaddr(xdst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() < 0) {
    printf("lock_client: call bind\n");
  }

  assert(pthread_mutex_init(&locks_cache_m, 0) == 0);
}

rlock_protocol::status
lock_client_cache::retry(int clt, lock_protocol::lockid_t lid, int & r)
{
  printf("%s: Received retry for lock %llu\n", id.c_str(), lid);

  ScopedLock l(&locks_cache_m);  
  struct lock_struct* lock = locks_cache[lid];


  ScopedLock l2(lock->mutex);  
  int ret = cl->call(lock_protocol::acquire, id, rlock_port, 0, lid, r);
  if (ret == lock_protocol::OK)
  {
    lock->lock_state = lock_struct::FREE;
    pthread_cond_signal(lock->cond);
  } 

  return rlock_protocol::OK;

}

rlock_protocol::status
lock_client_cache::revoke(int clt, lock_protocol::lockid_t lid, int & r)
{
  printf("%s: Received revoke for lock %llu\n", id.c_str(), lid);

  struct lock_struct* lock;
  {
    ScopedLock l(&locks_cache_m);  
    lock = locks_cache[lid];
  }


  ScopedLock l2(lock->mutex);  
  lock->revoke_requested = true;

  return rlock_protocol::OK;

}



void
lock_client_cache::releaser()
{

  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.
  while (true)
  {
    ScopedLock l(&releaser_m);
    while (release_queue.empty()) 
      assert(pthread_cond_wait(&releaser_cond, &releaser_m) >= 0);

    ScopedLock l2(&locks_cache_m);
    lock_protocol::lockid_t lid = release_queue.front();
    release_queue.pop();

    struct lock_struct* lock = locks_cache[lid];
    locks_cache.erase(lid);

    free_lock(lock);

    printf("%s: Sending release for lock %llu\n", id.c_str(), lid);

    int r;
    int ret = cl->call(lock_protocol::release, id, rlock_port, 0, lid, r);
    assert(ret == lock_protocol::OK);
  }

}


lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{

  
  printf("%s: acquiring %llu\n", id.c_str(), lid);
  struct lock_struct* rl;

  {    
    ScopedLock l(&locks_cache_m);  
    

    if (locks_cache.count(lid) == 0)
    {
      // Request the lock from server
      int r;
      int ret = cl->call(lock_protocol::acquire, id, rlock_port, 0, lid, r);
      int state = lock_struct::ACQUIRING;

      if (ret == lock_protocol::OK)
        state = lock_struct::FREE;

      rl = (struct lock_struct*)malloc(sizeof(struct lock_struct));
      pthread_mutex_t* mutex = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
      pthread_cond_t* cond = (pthread_cond_t*)malloc(sizeof(pthread_cond_t));
      assert(pthread_mutex_init(mutex, NULL) == 0);
      assert(pthread_cond_init(cond, NULL) == 0);
      rl->revoke_requested = false;
      rl->lock_state = state;
      rl->requests = 1;
      rl->mutex = mutex;
      rl->cond = cond;

      
      locks_cache[lid] = rl;

    }
    else
    {
      rl = locks_cache[lid]; // existing lock
      rl->requests++;
    }
  }


  {  
    ScopedLock l(rl->mutex);  

    // lock is taken, wait for it to free
    while (rl->lock_state != lock_struct::FREE) 
    {
      assert(pthread_cond_wait(rl->cond, rl->mutex) >= 0);
    }

    rl->lock_state = lock_struct::LOCKED;
    rl->current_owner = pthread_self();
    rl->requests--;

  }
  
  return lock_protocol::OK;  
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
    ScopedLock l(&locks_cache_m);  
    if (locks_cache.count(lid) == 0)
      return lock_protocol::NOENT;

    struct lock_struct* lock =  locks_cache[lid];
    
    
    ScopedLock l2(lock->mutex);  

    assert( pthread_equal(pthread_self(), lock->current_owner));

    lock->lock_state = lock_struct::FREE;

    if (lock->requests == 0 and lock->revoke_requested == true)
    {
      lock->lock_state = lock_struct::RELEASING;
      release_queue.push(lid);
      pthread_cond_signal(&releaser_cond);
    }
    else
      pthread_cond_signal(lock->cond);
    return lock_protocol::OK;

}

