#ifndef yfs_client_h
#define yfs_client_h

#include <string>
#include <vector>
#include <algorithm> 
#include <functional> 
#include <cctype>
#include <locale>
#include "extent_client.h"
#include "lock_client.h"

class yfs_client {
  extent_client *ec;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, FBIG, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    unsigned long long inum;
  };


// trim from start
static inline std::string &ltrim(std::string &s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
  return s;
}

// trim from end
static inline std::string &rtrim(std::string &s) {
  s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
  return s;
}

// trim from both ends
static inline std::string &trim(std::string &s) {
  return ltrim(rtrim(s));
}

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
  static inum i2bi(inum, int);
  lock_client *lc;
 public:
  static uint32_t i2f(inum); // converts a 64-bit inum to 32-bit fuse id
  static inum f2i(uint32_t); // converts a 32-bit fuse id to 64-bit inum

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);

  int getdircontents(inum, std::vector<dirent>&);
  int getdircontents_nonsafe(inum, std::vector<dirent>&);
  int lookup(inum, const char*, inum&);

  int createdir(inum, const char*, inum&);
  int createnode(inum, const char*, inum&);

  int write(inum, const char*, size_t, off_t);
  int read(inum, size_t, off_t, std::string&);

  int updatetime(inum);

  int unlink(inum, const char*,  bool do_not_lock=false);

  int setsize(inum, size_t);
  int getsize(inum, size_t &);
};

#endif 
