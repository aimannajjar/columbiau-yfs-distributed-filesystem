// the caching lock server implementation

#include "lock_server_cache.h"
#include "rpc/slock.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>


static void *
revokethread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->retryer();
  return 0;
}

lock_server_cache::lock_server_cache()
{
  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  assert (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  assert (r == 0);

  assert(pthread_mutex_init(&locks_table_m, 0) == 0);
  assert(pthread_mutex_init(&retry_m, 0) == 0);
  assert(pthread_mutex_init(&revoke_m, 0) == 0);
  assert(pthread_cond_init(&retry_cond, NULL) == 0);
  assert(pthread_cond_init(&revoke_cond, NULL) == 0);

}

lock_protocol::status 
lock_server_cache::stat(lock_protocol::lockid_t, int &)
{
  return lock_protocol::OK;
}

lock_protocol::status
lock_server_cache::release(std::string host, int port, int seq, lock_protocol::lockid_t lid, int &r)
{
  printf("Releasing %llu\n", lid);
  ScopedLock ml(&locks_table_m);
  locks_table[lid].lock_state = lock_server_cache::FREE;
  

  pthread_cond_signal(&retry_cond);
  return lock_protocol::OK;
}

lock_protocol::status
lock_server_cache::acquire(std::string host, int port, int seq, lock_protocol::lockid_t lid, int &r)
{
  printf("client %s acquiring %llu\n", host.c_str(), lid);
  
  ScopedLock ml(&locks_table_m);
  
  if (locks_table.count(lid) == 0 || locks_table[lid].lock_state == lock_server_cache::FREE)
  {
    if (locks_table.count(lid) == 0)
    {
      printf("Lock is new\n");
      struct lock_st lock;
      locks_table[lid] = lock;      
      locks_table[lid].requests = 1;
    }
    else
    {
      printf("Lock is free\n");
    }
    locks_table[lid].requests--;
    locks_table[lid].current_owner = host;
    locks_table[lid].lock_state = lock_server_cache::LOCKED;

    // remove this request from lock's queue (if it is there)
    // std::vector<qrequest> q = locks_table[lid].queued_requests;
    // std::vector<qrequest>::iterator it;

    // for(it = q.begin(); it < q.end(); it++){
    //   qrequest req = *it;
    //   if (req.host.compare(host) == 0)
    //   {
    //     q.erase(it++);
    //     printf("removed!!!\n");
    //     break;
    //   }
    // }

    


    if (locks_table[lid].requests > 0)
    {
      printf("OK NOCACHE (queued requests: %d)\n", locks_table[lid].requests);
      r = lock_protocol::NOCACHE;
    }
    else
    {
      printf("OK CACHABLE (queued requests: %d)\n", locks_table[lid].requests);
      r = lock_protocol::OK;
    }
    
    return r;
  }
  else
  {
    // Lock is cached somewhere, wake up revoker and queue request
    printf("Lock is cached somewhere. RETRY\n");


    // Add this client to this lock's queued requests
    struct qrequest qreq;
    qreq.host = host;
    qreq.lid = lid;
    qreq.seqno = seq;

    locks_table[lid].requests++;
    locks_table[lid].queued_requests.push(qreq);

    // Add lock to revoker queue and wake the thread
    struct qrequest req;
    req.host = locks_table[lid].current_owner;
    req.lid = lid;
    req.seqno = seq;
    revoke_queue.push(req);
    pthread_cond_signal(&revoke_cond);

    // Add to retryer queue (but don't wake it now)
    retry_list.push_back(lid);
    
    r = lock_protocol::RETRY;

    return lock_protocol::RETRY;
  }

}


void
lock_server_cache::revoker()
{

  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock
  while (true)
  {
    ScopedLock l(&revoke_m);
    while (revoke_queue.empty()) 
      assert(pthread_cond_wait(&revoke_cond, &revoke_m) >= 0);

    
    qrequest req = revoke_queue.front();
    revoke_queue.pop();

    printf("Sending revoke to %s for acquire with seq: %d\n", req.host.c_str(), req.seqno);


    sockaddr_in dstsock;
    make_sockaddr(req.host.c_str(), &dstsock);
    rpcc cl(dstsock);
    if (cl.bind() < 0) {
      printf("lock_client: call bind\n");    
      continue;
    }

    int r;
    cl.call(rlock_protocol::revoke, cl.id(), req.lid, req.seqno, r);
  }

}


void
lock_server_cache::retryer()
{


  while (true)
  {
    ScopedLock l(&retry_m);
    while (retry_list.empty()) 
      assert(pthread_cond_wait(&retry_cond, &retry_m) >= 0);

    std::vector<lock_protocol::lockid_t>::iterator it;

    for(it = retry_list.begin(); it < retry_list.end(); it++){
      lock_protocol::lockid_t lid = *it;

      if (locks_table[lid].lock_state != lock_server_cache::FREE)
        continue;

      
      

      printf("Sending retry to %s\n", locks_table[lid].queued_requests.front().host.c_str());
      qrequest req = locks_table[lid].queued_requests.front();
      locks_table[lid].queued_requests.pop();

      sockaddr_in dstsock;
      make_sockaddr(req.host.c_str(), &dstsock);
      rpcc cl(dstsock);
      if (cl.bind() < 0) {
        printf("lock_client: call bind\n");    
        continue;
      }

      retry_list.erase(it++);

      int r;
      cl.call(rlock_protocol::retry, cl.id(), lid, req.seqno, r);
    }
      

  }

}



