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
#include <vector>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  std::string buf;
  if (ec->get(1, buf) == extent_protocol::NOENT)
  {
    // Serialize an empty directory for root
    inum inum = 1;
    std::ostringstream ost;
    ost << inum;
    ost << " ";
    ost << std::string("root");
    int ret = ec->put(inum, ost.str());
    if (ret != extent_protocol::OK)
      printf("Error: could not initialize root directory\n");
  }

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
yfs_client::getdircontents(inum parent, std::vector<dirent>& list)
{
  printf("YFS::getdircontents(%llu)\n", parent);
  std::string val;
  if (ec->get(parent, val) != extent_protocol::OK) {
    printf("directory inum was not found..\n");
    return IOERR;
  }

  //  Deserialize direcory
  std::istringstream is(val);
  inum dir_inum;
  std::string dir_name;

  is >> dir_inum;
  is >> dir_name;

  while ( !is.eof() )
  {
    dirent entry;
    is >> entry.inum;
    is >> entry.name;
    list.push_back(entry);
  }
  return OK;

}

int
yfs_client::createdir(inum parent, const char* name, inum & out)
{
  int r = OK;
  
  uint32_t fuse_number = rand() & ~0x80000000;  // ensure first bit is not set
  inum inum = yfs_client::f2i(fuse_number);

  // Serialize an empty directory
  std::ostringstream ost;
  ost << inum;
  ost << " ";
  ost << std::string(name);
  int ret = ec->put(inum, ost.str());
  if (ret != extent_protocol::OK) {
    r = IOERR;
  }
  out = inum;

  // Append an entry to parent
  std::string parent_dir;
  ret = ec->get(parent, parent_dir);
  std::ostringstream os;
  os << parent_dir;
  os << " ";
  os << inum;
  os << " ";
  os << std::string(name);
  ret = ec->put(parent, os.str());
  if (ret != extent_protocol::OK)
    r = IOERR;

  return r;
}

int
yfs_client::createnode(inum parent, const char* name, inum & out)
{
  int r = OK;
  
  uint32_t fuse_number = rand() | 0x80000000; 
  inum inum = yfs_client::f2i(fuse_number);

  // Serialize an empty file
  std::ostringstream ost;
  ost << inum;
  ost << " ";
  ost << std::string(name);
  int ret = ec->put(inum, ost.str());
  if (ret != extent_protocol::OK) {
    r = IOERR;
  }
  out = inum;

  // Append an entry to parent
  std::string parent_dir;
  ret = ec->get(parent, parent_dir);
  std::ostringstream os;
  os << parent_dir;
  os << " ";
  os << inum;
  os << " ";
  os << std::string(name);
  ret = ec->put(parent, os.str());
  if (ret != extent_protocol::OK)
    r = IOERR;

  return r;
}


int
yfs_client::lookup(inum parent, const char* name, inum & out)
{

  printf("yfs::lookup(parent=%llu, name=%s) = \n", parent, name);
  int r = NOENT;
  std::string val;
  if (ec->get(parent, val) != extent_protocol::OK) {
    printf("parent not found..\n");
    r = IOERR;
  }  

  printf("%s.. let's deserialize:\n", val.c_str());

  inum parent_inum;
  std::string parent_name;

  std::istringstream is(val);
  is >> parent_inum;
  is >> parent_name;
  
  printf("parent inum: %llu\nparent name:%s\ncontents:\n", 
          parent_inum, parent_name.c_str());

  std::string target_name(name);

  while (!is.eof())
  {
    inum child_inum;
    std::string child_name;
    is >> child_inum;
    is >> child_name;

    printf("  found entry: %llu, '%s' ..", child_inum, child_name.c_str());
    if (target_name.compare(child_name) == 0)
    {
      // found!
      printf(" found it!\n");
      out = child_inum;
      r = OK;
      break;
    }
    printf(" that's not it.\n");

  }

  printf("done..\n");

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



