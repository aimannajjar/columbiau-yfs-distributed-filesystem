#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#line 6 "../lock_server_cache.h"
#include <map>
#include <queue>
#line 8 "../lock_server_cache.h"
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#line 14 "../lock_server_cache.h"

#line 18 "../lock_server_cache.h"

#line 50 "../lock_server_cache.h"

#line 54 "../lock_server_cache.h"

struct qrequest
{
    std::string host;
    int seqno;
    lock_protocol::lockid_t lid;
};

struct lock_st
{
  enum lock_state { FREE, LOCKED };
  typedef int state;
  int lock_state;
  std::string current_owner;
  int requests; /* number of clients waiting for this lock */
  std::queue<qrequest> queued_requests;
};


class lock_server_cache {
#line 56 "../lock_server_cache.h"
 public:
    enum lock_state { FREE, LOCKED };
 private:
    pthread_mutex_t locks_table_m;
    std::map<lock_protocol::lockid_t, struct lock_st> locks_table;
    std::vector<lock_protocol::lockid_t> retry_list;
    std::queue<qrequest> revoke_queue;
    std::map<std::string, rpcc*> rpc_clients;
    pthread_cond_t retry_cond;
    pthread_cond_t revoke_cond;
    pthread_mutex_t retry_m;
    pthread_mutex_t revoke_m;

#line 88 "../lock_server_cache.h"
 public:
#line 92 "../lock_server_cache.h"
  lock_server_cache();
#line 94 "../lock_server_cache.h"
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  lock_protocol::status acquire(std::string, int, int, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(std::string host, int port, int seq, lock_protocol::lockid_t lid, int &r);

  void revoker();
  void retryer();
#line 109 "../lock_server_cache.h"
};

#endif
