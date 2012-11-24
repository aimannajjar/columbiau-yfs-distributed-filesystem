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

  assert(pthread_mutex_init(&m_, 0) == 0);
}

lock_protocol::status
lock_server::acquire(int clt, int seq, lock_protocol::lockid_t lid, int &r)
{
  ScopedLock ml(&locks_table_m);
  
  if (locks_table.count(lid) == 0 || locks_table[lid] == FREE)
  {
    locks_table[lid] = LOCKED;
    return lock_protocol::OK;
  }
  else
  {
    return lock_protocol::RETRY;
  }

}


void
lock_server_cache::revoker()
{

  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock

}


void
lock_server_cache::retryer()
{

  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.

}



