// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

extent_server::extent_server() {}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  printf("extent_server::put(%llu, %s);\n", id, buf.data());
  store[id] = buf;
  if (attr_store.count(id) == 0)
  { 
      extent_protocol::attr nattr;
      nattr.ctime = time(NULL);
      attr_store[id] = nattr;
    }

    attr_store[id].size = buf.size();
    attr_store[id].atime = time(NULL);
    attr_store[id].mtime = time(NULL);

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server::get(%llu) = ", id);
  if (store.count(id) > 0)
  {
    buf = store[id];    
    printf("%s\n", buf.c_str());
  }
  else
  {
    printf("not found\n");
    return extent_protocol::NOENT;
  }
  
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server::getattr(%llu).size = ", id);
  a.size = 0;
  a.atime = 0;
  a.mtime = 0;
  a.ctime = 0;

  if (attr_store.count(id) > 0)
  {
    printf("%d\n", attr_store[id].size);
    a.size = attr_store[id].size;
    a.atime = attr_store[id].atime;
    a.mtime = attr_store[id].mtime;
    a.ctime = attr_store[id].ctime;
    return extent_protocol::OK;
  }
  else
  {
    printf("not found\n");
    return extent_protocol::NOENT;
  }

  
}

int extent_server::setattr(extent_protocol::extentid_t id, extent_protocol::attr a)
{
  printf("extent_server::setattr(%llu,  size: %d):  ", id, a.size);

  if (attr_store.count(id) > 0)
  {
    printf("success. old size was %d\n", attr_store[id].size);

    attr_store[id].size = a.size;
    return extent_protocol::OK;
  }
  else
  {
    printf("not found\n");
    return extent_protocol::NOENT;
  }
  
}


int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  store.erase(id);
  return extent_protocol::OK;
}

