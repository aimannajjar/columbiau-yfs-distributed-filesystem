// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  store[id] = buf;
  if (attr_store.count(id) == 0)
  { 
      extent_protocol::attr nattr;
      nattr.ctime = std::time(0);
      attr_store[id] = nattr;
    }

    attr_store[id].size = buf.size();
    attr_store[id].atime = std::time(0);
    attr_store[id].mtime = std::time(0);

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  buf = store[id];
  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  a.size = 0;
  a.atime = 0;
  a.mtime = 0;
  a.ctime = 0;

  if (attr_store.count(id) > 0)
  {
    a.size = attr_store[id].size;
    a.atime = attr_store[id].atime;
    a.mtime = attr_store[id].mtime;
    a.ctime = attr_store[id].ctime;
  }

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  store.erase(id);
  return extent_protocol::OK;
}

