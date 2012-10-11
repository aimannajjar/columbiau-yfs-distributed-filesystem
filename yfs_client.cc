// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);

}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

uint32_t
yfs_client::i2f(inum inum)
{
  // converts a 64-bit inum to 32-bit fuse id
  return inum & 0xffffffff;
}

yfs_client::inum 
yfs_client::f2i(uint32_t fuseid) 
{
  // converts a 32-bit fuse id to 64-bit inum
  uint64_t inum = fuseid;
  return inum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;


  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

int
yfs_client::createdir(inum parent, const char* name)
{
  int r = OK;
  
  uint32_t fuse_number = rand() & ~0x80000000; 
  inum inum = yfs_client::f2i(fuse_number);

  // Serialize an empty directory
  std::ostringstream ost;
  uint32_t total_entries = 0;
  ost << inum;
  ost << std::string(name);
  ost << total_entries;

  if (ec->put(inum, ost.str()) != extent_protocol::OK) {
    r = IOERR;
  }
  return r;
}

int
yfs_client::lookup(inum parent, const char* name, inum & out)
{
  int r = NOENT;
  std::string val;
  if (ec->get(parent, val) != extent_protocol::OK) {
    r = IOERR;
  }  

  inum parent_inum;
  std::string parent_name;
  uint32_t total_entries;

  std::istringstream is(val);
  is >> parent_inum;
  is >> parent_name;
  is >> total_entries;

  std::string target_name(name);

  for (int i=0; i<total_entries;i++)
  {
    inum child_inum;
    std::string child_name;
    is >> child_inum;
    is >> child_name;
    if (target_name.compare(child_name) == 0)
    {
      // found!
      out = child_inum;
      r = OK;
      break;
    }

  }

  return r;
  
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;


  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}



