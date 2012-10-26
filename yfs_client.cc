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
#include <math.h>
#include <algorithm>

#define BLOCK_SIZE 1024.0

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

yfs_client::inum 
yfs_client::i2bi(inum inum, int block_no) 
{
  // constructs the inum of a block by file's root inum and block number
  int64_t key = block_no;
  key = (key << 32) | inum;   
  return key;
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

  size_t sz;
  if (getsize(inum, sz) != OK)
  {
    r = IOERR;
    goto release;
  }


  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = sz;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

int
yfs_client::getsize(inum inum, size_t & size)
{
  printf("YFS::getsize(%llu)\n", inum);

  // -----------------------
  // Calculate current size
  // -----------------------
  // First block size
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) 
    return IOERR;

  size = a.size;

  // Now add up next blocks sizes
  int i = 1;
  int64_t key = yfs_client::i2bi(inum, i);
  extent_protocol::status r;
  while ( (r = ec->getattr(key, a)) != extent_protocol::NOENT) 
  {
    if (r != extent_protocol::OK)
      return IOERR; // unexpected error type;

    size += a.size;
    i++;
    key = yfs_client::i2bi(inum, i);
  }
  return OK;

}

int 
yfs_client::setsize(inum inum, size_t target_size)
{

  printf("YFS::setsize(%llu, %lu)\n", inum, target_size);

  // -----------------------
  // Calculate current size
  // -----------------------

  // First block size
  extent_protocol::attr a;
  size_t size;
  if (ec->getattr(inum, a) != extent_protocol::OK) 
    return IOERR;

  size = a.size;
  size_t last_block_size = a.size;

  // Now add up next blocks sizes
  int i = 1;
  int64_t key = yfs_client::i2bi(inum, i);

  extent_protocol::status r;
  while ( (r = ec->getattr(key, a)) != extent_protocol::NOENT) 
  {
    if (r != extent_protocol::OK)
      return IOERR; // unexpected error type;

    size += a.size;
    last_block_size = a.size;
    i++;
    key = yfs_client::i2bi(inum, i);
  }

  if (target_size > size)
  {
    // Expand size
    int remaining_new_size = target_size - size;
    int curr_block = i;
    int avail_size = BLOCK_SIZE;

    // Can we use some space from last block ?
    if (last_block_size < BLOCK_SIZE)
    {
      curr_block = i - 1;
      avail_size = BLOCK_SIZE - last_block_size;
    }
    
    while (remaining_new_size > 0)
    {
      key = yfs_client::i2bi(inum, curr_block);
      std::ostringstream os;
      if (avail_size < BLOCK_SIZE)
      {
        // We are appending to an exisiting block, we need to fetch existing data
        std::string val;
        if (ec->get(key, val) != extent_protocol::OK) 
          return IOERR;
        os << val;        
      }

      // Let's fill up remaining space with 0
      int new_data_size = std::min(avail_size, remaining_new_size);
      char filler[new_data_size];
      memset(filler, '\0', new_data_size);
      os << filler;
      if (ec->put(key, os.str().data()) != extent_protocol::OK) 
        return IOERR;      

      remaining_new_size -= new_data_size;

      // For next iteration (new block)
      avail_size = avail_size - BLOCK_SIZE;
      curr_block++;
    }
    

  }

  return OK;




 
}

int
yfs_client::write(inum inum, const char* c_contents, size_t size, off_t offset)
{
  printf("YFS::write(%llu, %ld, %lu)\n", inum, offset, size);
  // Determine the first block to write at
  int start = (int)floor(offset / BLOCK_SIZE);

  // How many blocks?
  int total_blocks = (int)ceil(size / BLOCK_SIZE);

  // Offset for first block
  int first_offset = offset - (start * BLOCK_SIZE);

  // Retrieve pre-offset data for first block (if needed)
  std::string preoffset;
  if (first_offset > 0)
  {
    int64_t key = yfs_client::i2bi(inum, start);

    std::string val;
    if (ec->get(key, val) != extent_protocol::OK) 
      return IOERR;

    preoffset.append(val.substr(0, first_offset));

  }

  // Write to blocks
  std::string contents(c_contents);
  printf("   first block: %d, total_blocks: %d\n", start, total_blocks);
  for (int i = start; i<start+total_blocks;i++)
  {
    yfs_client::inum key = yfs_client::i2bi(inum, i);
    
    std::ostringstream os;
    if (first_offset != 0)
      os << preoffset;


    os << contents.substr(i-start, BLOCK_SIZE - first_offset);

    printf("Writing %s to block %d of inum %llu\n", os.str().data(), i, key);
    int ret = ec->put(key, os.str());
    if (ret != extent_protocol::OK) {
      return IOERR;
    }
    first_offset = 0;

  }

  return OK;

}


int
yfs_client::read(inum inum, size_t size, off_t offset, std::string& out)
{
  printf("YFS::read(%llu, %ld, %lu)\n", inum, offset, size);
  // Determine the first block to read from
  int start = (int)floor(offset / BLOCK_SIZE);

  // How many blocks?
  int total_blocks = (int)ceil(size / BLOCK_SIZE);

  // Offset for first block
  int first_offset = offset - (start * BLOCK_SIZE);


  // Read blocks
  std::ostringstream os;
  for (int i = start; i<start+total_blocks-1;i++)
  {
    yfs_client::inum key = yfs_client::i2bi(inum, i);

    std::string val;

    int ret = ec->get(key, val);
    if (ret != extent_protocol::OK) {
      return IOERR;
    }

    std::string substr;
    if (first_offset != 0)
      substr = val.substr(first_offset, BLOCK_SIZE - first_offset);
    else
      substr = val;


    printf("Just read block number: %d (block unique key: %llu): %s\n", i, key, substr.data());
    first_offset = 0;

  }

  out = os.str(); 

  return OK;

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

  printf("YFS::createnode(parent=%llu, %s)\n", parent, name);

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



