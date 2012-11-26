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

  seqno = 1;
  acked_seqno = 0;

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
lock_client_cache::retry(int clt, lock_protocol::lockid_t lid, int seq, int & r)
{
  printf("%s: Received retry for lock %llu in response to seq: %d, current seq is: %d\n", id.c_str(), lid, seq, seqno);

  ScopedLock l(&locks_cache_m);  
  if (locks_cache.count(lid) == 0)
    return rlock_protocol::NOENT;

  struct lock_struct* lock = locks_cache[lid];


  ScopedLock l2(lock->mutex);  
  int r2;
  int ret = cl->call(lock_protocol::acquire, id, rlock_port, seqno++, lid, r2);
  if (ret == lock_protocol::OK || ret == lock_protocol::NOCACHE)
  {
    if (ret == lock_protocol::NOCACHE)
    {
      printf("NOCACHE returned\n");
      lock->revoke_requested = true;
    }
    else
    {
      printf("CACHABLE\n");
      lock->revoke_requested = false;
    }

    lock->lock_state = lock_struct::FREE;
    pthread_cond_signal(lock->cond);
  } 
  r = rlock_protocol::OK;
  return rlock_protocol::OK;

}

rlock_protocol::status
lock_client_cache::revoke(int clt, lock_protocol::lockid_t lid, int seq, int & r)
{
  ScopedLock l(&locks_cache_m);  

  printf("%s: Received revoke for lock %llu in response to seq: %d, current seq is: %d\n", id.c_str(), lid, seq, seqno);
  if (locks_cache.count(lid) == 0)
  {
    printf("%s:revoke: lock %llu does not exist, probably already revoked previously\n", id.c_str(), lid);
    return rlock_protocol::NOENT;
  }

  struct lock_struct* lock = locks_cache[lid];

  
  ScopedLock l2(lock->mutex);  
  lock->revoke_requested = true;

  if (lock->requests == 0 && lock->lock_state == lock_struct::FREE)
  {
      ScopedLock l3(&releaser_m);
      lock->lock_state = lock_struct::RELEASING;
      release_queue.push(lock);
      locks_cache.erase(lid);
      pthread_cond_signal(&releaser_cond);    
  }

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
    struct lock_struct* lock = release_queue.front();
    release_queue.pop();

    printf("%s: Sending release for lock %llu\n", id.c_str(), lock->lid);

    int r;
    int ret = cl->call(lock_protocol::release, id, rlock_port, 0, lock->lid, r);

    // locks_cache.erase(lid);
    free_lock(lock);

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
      printf("%s: sending acquire for lock %llu with seq number: %d\n", id.c_str(), lid, (seqno+1));
      int r;
      int ret = cl->call(lock_protocol::acquire, id, rlock_port, seqno++, lid, r);
      int state = lock_struct::ACQUIRING;

      if (ret == lock_protocol::OK || ret == lock_protocol::NOCACHE)
        state = lock_struct::FREE;

      rl = (struct lock_struct*)malloc(sizeof(struct lock_struct));
      pthread_mutex_t* mutex = (pthread_mutex_t*)malloc(sizeof(pthread_mutex_t));
      pthread_cond_t* cond = (pthread_cond_t*)malloc(sizeof(pthread_cond_t));
      assert(pthread_mutex_init(mutex, NULL) == 0);
      assert(pthread_cond_init(cond, NULL) == 0);
      if (ret == lock_protocol::NOCACHE)
      {
        rl->revoke_requested = true;
        printf("%s:acquire: server says don't cache lock %llu\n", id.c_str(), lid);
      }
      else if (ret == lock_protocol::OK)
      {
        printf("%s:acquire: server says you can cache lock %llu\n", id.c_str(), lid);
        rl->revoke_requested = false;
      }
      else
      {
        printf("%s:acquire: server says wait for a retry call for lock %llu\n", id.c_str(), lid);
      }
      rl->lid = lid;
      rl->lock_state = state;
      rl->requests = 1;
      rl->mutex = mutex;
      rl->cond = cond;

      
      locks_cache[lid] = rl;
      acked_seqno = seqno - 1;
    }
    else
    {
      printf("%s:acquire: I currently own this lock %llu; not calling the server\n", id.c_str(), lid);
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
    printf("%s: acquired lock %llu\n", id.c_str(), lid);

  }
  
  return lock_protocol::OK;  
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
    printf("%s: releasing %llu\n", id.c_str(), lid);
    ScopedLock l(&locks_cache_m);  
    if (locks_cache.count(lid) == 0)
    {
      printf("%s:lock %llu doesn't exit in cache!\n", id.c_str(), lid);
      return lock_protocol::NOENT;
    }

    struct lock_struct* lock =  locks_cache[lid];
    
    
    ScopedLock l2(lock->mutex);  
    printf("%s:release: lock=%llu; revoke requested=%d; backlog_requests=%d\n", id.c_str(), lid, lock->revoke_requested, lock->requests);

    assert( pthread_equal(pthread_self(), lock->current_owner));

    lock->lock_state = lock_struct::FREE;

    if (lock->requests == 0 and lock->revoke_requested == true)
    {

      ScopedLock l3(&releaser_m);
      printf("%s: granting lock %llu back to server\n",id.c_str(), lid);
      lock->lock_state = lock_struct::RELEASING;
      release_queue.push(lock);
      locks_cache.erase(lid);
      pthread_cond_signal(&releaser_cond);
    }
    else
      pthread_cond_signal(lock->cond);
    return lock_protocol::OK;

}

