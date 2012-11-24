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
  lc = new lock_client(lock_dst);
  std::string buf;
  if (ec->get(1, buf) == extent_protocol::NOENT)
  {
    // Serialize an empty directory for root
    inum inum = 1;
    std::ostringstream os;
    os << inum;
    int ret = ec->put(inum, os.str());
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
yfs_client::unlink(inum parent, const char* name, bool do_not_lock)
{
  // If do_not_lock is set, assumes parent lock is acquired
  if (!do_not_lock) lc->acquire(parent);
  yfs_client::inum inum;
  yfs_client::status ret = lookup(yfs_client::f2i(parent), name, inum);
  if (ret == extent_protocol::NOENT)
  {
    if (!do_not_lock) lc->release(parent);
    return NOENT;
  }
  else if (ret != extent_protocol::OK)
  {
    if (!do_not_lock) lc->release(parent);
    return IOERR;
  } 
  lc->acquire(inum);


  yfs_client::status r = OK;
  if (isdir(inum))
  {

    // First remove dir contents:
    std::vector<yfs_client::dirent> v;
    getdircontents_nonsafe(inum, v);

    std::vector<yfs_client::dirent>::iterator it;
    for (it = v.begin(); it < v.end(); it++)
    {
      yfs_client::dirent entry = *it;
      if (unlink(inum, entry.name.c_str(), do_not_lock) != yfs_client::OK)
      {
        r = IOERR;
        break;
      }
    }
    if (r != OK)
    {
      lc->release(inum);
      if (!do_not_lock) lc->release(parent);
      return r;
    }

    // Remove directory itself
    extent_protocol::status r = ec->remove(inum);
    if (r == extent_protocol::OK)
      r = OK;
    else if (r == extent_protocol::NOENT)
    {
      r =  NOENT;
      lc->release(inum);
      if (!do_not_lock) lc->release(parent);
      return r;
    }
    else
    {
      r = IOERR;
      lc->release(inum);
      if (!do_not_lock) lc->release(parent);
      return r;
    }
  }
  else
  {
    r = ec->remove(inum) != extent_protocol::OK;
    if (r == extent_protocol::OK)
      r = OK;
    else if (r == extent_protocol::NOENT)
    {
      r = NOENT;
      lc->release(inum);
      if (!do_not_lock) lc->release(parent);      
      return r;
    }
    else
    {
      r = IOERR;
      lc->release(inum);
      if (!do_not_lock) lc->release(parent);      
      return r;
    }

    // Now remove next blocks for this file
    int i = 1;
    int64_t key = yfs_client::i2bi(inum, i);
    extent_protocol::status r;
    extent_protocol::attr a;
    while ( (r = ec->getattr(key, a)) != extent_protocol::NOENT) 
    {
      if (r != extent_protocol::OK)
      {
        lc->release(inum);
        if (!do_not_lock) lc->release(parent);
        return IOERR; // unexpected error type;
      }

      if (ec->remove(inum) != extent_protocol::OK)
      {
        lc->release(inum);
        if (!do_not_lock) lc->release(parent);
        return IOERR;
      }

      i++;
      key = yfs_client::i2bi(inum, i);
    }

  }


  // Now remove this entry from parents content
  std::vector<yfs_client::dirent> v;
  getdircontents_nonsafe(parent, v);

  std::ostringstream os;
  std::vector<yfs_client::dirent>::iterator it;
  os << parent;
  for (it = v.begin(); it < v.end(); it++)
  {
    yfs_client::dirent entry = *it;
    if (entry.inum != inum)
    {
      os << " ";
      os << entry.inum;
      os << " ";
      os << entry.name;
    }
  }
  std::string contents(os.str());
  ec->put(parent, trim(contents));
  printf("New directory contents: %s\n", trim(contents).c_str());

  lc->release(inum);
  if (!do_not_lock) lc->release(parent);

  return r;

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
  else if (target_size < size)
  {
    printf("    truncating size from %lu to %lu\n", size, target_size);
    // This one is easier, we iterate through current blocks
    // until we reach desired size, then we remove remaining blocks
    int curr_block = -1;
    size_t remaining_size = target_size;
    while (true)
    {
      printf("    remaining size %lu\n", remaining_size);
      curr_block++;
      yfs_client::inum key = yfs_client::i2bi(inum, curr_block);
      extent_protocol::attr a;
      extent_protocol::status r = ec->getattr(key, a);
      if (r != extent_protocol::NOENT)
      {
        if (r != extent_protocol::OK)
          return IOERR;

        if (remaining_size <= 0) // we've already reached desired size, remove block
        {
          printf("    removing block %d\n", curr_block);
          if (ec->remove(key) != extent_protocol::OK)
            return IOERR;
          continue;
        }

        if (a.size > remaining_size)
        {
          printf("    truncating block %d to %lu\n", curr_block, remaining_size);
          // truncate this block 
          std::string val;
          if (ec->get(key, val) != extent_protocol::OK) 
            return IOERR;

          if (ec->put(key, val.substr(0, remaining_size)) != extent_protocol::OK)
            return IOERR;

        }
        else
        {
          printf("    keeping block %d\n", curr_block);
          // keep this block, but subtract from remaining size
          remaining_size -= a.size;
        }


      }
      else
      {
        printf("    reached last block (total blocks = %d)\n", curr_block);
        break; // we've reached last block
      }
    }

  }

  return OK;




 
}

int
yfs_client::updatetime(inum inum)
{
  printf("Updating time for %llu\n", inum);
  std::string firstblock;
  if (ec->get(inum, firstblock) != extent_protocol::OK)
    return IOERR;
  
  if (ec->put(inum, firstblock) != extent_protocol::OK)
    return IOERR;

  return OK;  
}


int
yfs_client::write(inum inum, const char* c_contents, size_t size, off_t offset)
{
  printf("YFS::write(%llu, %ld, %lu)\n", inum, offset, size);

  // Determine the first block to write at
  int start = (int)floor(offset / BLOCK_SIZE);

  // Offset for first block
  int first_offset = offset - (start * BLOCK_SIZE);
  printf("    first block: %d, first_offset: %d\n", start, first_offset);

  // Write to blocks
  std::string contents(c_contents, size);
  int remaining_size = size;

  int curr_block = start;
  int total_written = 0;
  int i=0;
  while (remaining_size > 0)
  {
    yfs_client::inum key = yfs_client::i2bi(inum, curr_block);
    
    // Get existing data in the block
    std::string datastr;
    int ret = ec->get(key, datastr);
    if (ret != extent_protocol::OK && ret != extent_protocol::NOENT) {
      return IOERR;
    }

    // determine how much of the buffer we want to copy
    int written_bytes = std::min((int)(BLOCK_SIZE - first_offset), remaining_size);
    printf("    writing %d bytes to block %d of inum %llu:\n", written_bytes, curr_block, inum);
    printf("    contents.substring(%d, %d) of %lu\n", total_written, written_bytes,contents.size());

    // allocate a tmp buffer to copy from
    char tmp_buf[written_bytes];
    memset(tmp_buf, '\0', written_bytes);
    memcpy(tmp_buf, contents.data()+total_written, written_bytes);
    
    // allocate a buffer for complete data
    size_t datasize = std::max((int)datastr.size(), first_offset+written_bytes);
    char databuf[datasize];
    memcpy(databuf, datastr.data(), datastr.size()); // copy exiting data
    memcpy(databuf+first_offset, tmp_buf, written_bytes); // copy new data


    // Create an std::string object
    std::string value(databuf, datasize);


    // just for output 
    std::string sub(tmp_buf, written_bytes);
    printf("%s\n", sub.c_str());

    
    ret = ec->put(key, value);
    if (ret != extent_protocol::OK) {
      return IOERR;
    }
    // update numbers
    remaining_size -= written_bytes;
    total_written += written_bytes;
    first_offset = 0;    
    curr_block++;
    i++;

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
  printf("   first block: %d, total_blocks: %d\n", start, total_blocks);  
  for (int i = start; i<start+total_blocks-1;i++)
  {
    yfs_client::inum key = yfs_client::i2bi(inum, i);

    std::string val;

    int ret = ec->get(key, val);
    if (ret == extent_protocol::NOENT)
      break; // done
    else if (ret != extent_protocol::OK)
      return IOERR;

    std::string substr;
    if (first_offset != 0)
      substr = val.substr(first_offset, BLOCK_SIZE - first_offset);
    else
      substr = val;


    printf("    just read block number: %d (block unique key: %llu): %s\n", i, key, substr.data());
    os << substr.data();
    first_offset = 0;

  }

  printf("    file contents:%s\n", os.str().data());
  out = os.str(); 

  return OK;

}

int
yfs_client::getdircontents(inum parent, std::vector<dirent>& list)
{
  lc->acquire(parent);
  int r =getdircontents_nonsafe(parent, list);
  lc->release(parent);
  return r;

}

int
yfs_client::getdircontents_nonsafe(inum parent, std::vector<dirent>& list)
{
  // Assumes lock is acquired

  printf("YFS::getdircontents(%llu)\n", parent);
  std::string val;
  if (ec->get(parent, val) != extent_protocol::OK) {
    printf("directory inum was not found..\n");
    {
      return IOERR;
    }
  }

  //  Deserialize direcory
  std::istringstream is(trim(val));
  printf("Deserializing %s\n", val.c_str());
  yfs_client::inum myinum;
  is >> myinum;

  while ( !is.eof() )
  {
    dirent entry;
    is >> entry.inum;
    is >> entry.name;
    list.push_back(entry);
    if (trim(entry.name).empty())
      continue;
    printf("Added %s\n", entry.name.c_str());
  }

  return OK;

}

int
yfs_client::createdir(inum parent, const char* name, inum & out)
{
  int r = OK;
  
  lc->acquire(parent);

  uint32_t fuse_number = rand() & ~0x80000000;  // ensure first bit is not set
  inum inum = yfs_client::f2i(fuse_number);

  // Serialize an empty directory
  std::ostringstream oss;
  oss << inum;
  int ret = ec->put(inum, oss.str());
  if (ret != extent_protocol::OK) {
    lc->release(parent);
    return IOERR;
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
  std::string contents(os.str());
  ret = ec->put(parent, trim(contents));
  if (ret != extent_protocol::OK)
    r = IOERR;

  lc->release(parent);
  return r;
}

int
yfs_client::createnode(inum parent, const char* name, inum & out)
{

  printf("YFS::createnode(parent=%llu, %s)\n", parent, name);
  lc->acquire(parent);

  // Let's check if node exists already
  yfs_client::inum existing_num = 0;
  yfs_client::status lookret = lookup(parent, name, existing_num);


  int r = OK;
  yfs_client::inum inum = existing_num;
  if (lookret == yfs_client::NOENT)
  {
    uint32_t fuse_number = rand() | 0x80000000; 
    inum = yfs_client::f2i(fuse_number);
  }
  lc->acquire(inum);

  // Serialize an empty file
  std::string empty;
  int ret = ec->put(inum, empty);
  if (ret != extent_protocol::OK) {
    r = IOERR;
    lc->release(inum);
    lc->release(parent);
    return r;
  }
  out = inum;


  // Append an entry to parent if neccessary
  if (lookret == yfs_client::NOENT)
  {
    std::string parent_dir;
    ret = ec->get(parent, parent_dir);
    std::ostringstream os;
    os << parent_dir;
    os << " ";
    os << inum;
    os << " ";
    os << std::string(name);
    std::string contents(os.str());
    ret = ec->put(parent, trim(contents));
    if (ret != extent_protocol::OK)
      r = IOERR;
  }
  lc->release(inum);
  lc->release(parent);

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

  std::istringstream is(val);
  printf("contents:\n");

  std::string target_name(name);
  yfs_client::inum parentinum;
  is >> parentinum;

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



