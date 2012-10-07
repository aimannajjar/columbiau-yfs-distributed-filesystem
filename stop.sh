#!/usr/bin/env bash

if [ -z $MOUNTDIR ]; then
    MOUNTDIR=$PWD
fi

YFSDIR1=$MOUNTDIR/yfs1
YFSDIR2=$MOUNTDIR/yfs2

export PATH=$PATH:/usr/local/bin
UMOUNT="umount"
if [ -f "/usr/local/bin/fusermount" -o -f "/usr/bin/fusermount" -o -f "/bin/fusermount" ]; then
    UMOUNT="fusermount -u";
fi
$UMOUNT $YFSDIR1
$UMOUNT $YFSDIR2
killall extent_server
killall yfs_client
killall lock_server
