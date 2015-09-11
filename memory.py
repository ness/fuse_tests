#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT, ENODATA
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time, sleep

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

logging.basicConfig()
logger = logging.getLogger(__name__)

if not hasattr(__builtins__, 'bytes'):
    bytes = str


class FSNode(object):
    def __init__(self, mode=0):
        self.mode = mode
        self.ctime = time()
        self.mtime = time()
        self.atime = time()
        self.nlink = 0
        self.uid = 0
        self.gid = 0

        self.xattrs = {}

    @property
    def attrs(self):
        return dict(
            st_mode=self.mode,
            st_ctime=self.ctime,
            st_mtime=self.mtime,
            st_atime=self.atime,
            st_nlink=self.nlink,
            st_uid=self.uid,
            st_gid=self.gid,
            st_size=self.size,
        )


class NoneNode(FSNode):
    @property
    def attrs(self):
        raise FuseOSError(ENOENT)


class DirNode(FSNode):
    def __init__(self, *args, **kwargs):
        super(DirNode, self).__init__(*args, **kwargs)
        self.mode = (S_IFDIR | self.mode)
        self.nlink = 2
        self.entries = dict()
        self.size = 0

    def find_node(self, path):
        path_parts = path.split('/')[1:]
        node = self
        for part in path_parts:
            if part == '':
                continue
            node = node.entries.get(part, NoneNode())
        return node

class FileNode(FSNode):
    def __init__(self,  *args, **kwargs):
        super(FileNode, self).__init__(*args, **kwargs)
        self.nlink = 1
        self.mode = (S_IFREG | self.mode)
        self.data = b''

    @property
    def size(self):
        return len(self.data)


class SymLinkNode(FileNode):
    def __init__(self,  *args, **kwargs):
        super(SymLinkNode, self).__init__(*args, **kwargs)
        self.mode = (S_IFLNK | self.mode)


class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        self.fs_root = DirNode(mode=0755)

    def chmod(self, path, mode):
        node = self.get_node(path)
        node.mode &= 0770000
        node.mode |= mode
        return 0

    def chown(self, path, uid, gid):
        node = self.get_node(path)
        node.uid = uid
        node.gid = gid

    def create(self, path, mode):
        parent_path, filename = self.split_parent_and_filename(path)
        parent_node = self.get_node(parent_path)
        parent_node.entries[filename] = FileNode(mode=mode)
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        return self.get_node(path).attrs

    def getxattr(self, path, name, position=0):
        node = self.get_node(path)
        try:
            return node.xattrs[name]
        except KeyError:
            raise FuseOSError(ENODATA)       # Should return ENOATTR

    def listxattr(self, path):
        node = self.get_node(path)
        return node.xattrs.keys()

    def mkdir(self, path, mode):
        parent_path, filename = self.split_parent_and_filename(path)
        parent_node = self.get_node(parent_path)
        parent_node.entries[filename] = DirNode(mode=mode)
        parent_node.nlink += 1

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        node = self.get_node(path)
        return node.data[offset:offset + size]

    def readdir(self, path, fh):
        node = self.get_node(path)
        content = ['.', '..'] + node.entries.keys()
        return content

    def readlink(self, path):
        node = self.get_node(path)
        return node.data

    def removexattr(self, path, name):
        attrs = self.files[path].get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old_path, new_path):
        # TODO: update nlink on old dir and on new - needed when moving_node is a dir
        moving_node = self.get_node(old_path)

        parent_path, filename = self.split_parent_and_filename(new_path)
        parent_node = self.get_node(parent_path)
        parent_node.entries[filename] = moving_node

        # remove old dir entry
        parent_path, filename = self.split_parent_and_filename(old_path)
        parent_node = self.get_node(parent_path)
        del parent_node.entries[filename]

    def rmdir(self, path):
        parent_path, filename = self.split_parent_and_filename(path)
        parent_node = self.get_node(parent_path)
        del parent_node.entries[filename]
        parent_node.nlink -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        node = self.get_node(path)
        node.xattrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        parent_path, filename = self.split_parent_and_filename(target)
        parent_node = self.get_node(parent_path)
        symlink = SymLinkNode(mode=0777)
        symlink.data = source
        parent_node.entries[filename] = symlink

    def truncate(self, path, length, fh=None):
        node = self.get_node(path)
        node.data = node.data[:length]

    def unlink(self, path):
        parent_path, filename = self.split_parent_and_filename(path)
        parent_node = self.get_node(parent_path)
        del parent_node.entries[filename]

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        node = self.get_node(path)
        node.atime = atime
        node.mtime = mtime

    def write(self, path, data, offset, fh):
        node = self.get_node(path)
        node.data = node.data[:offset] + data
        return len(data)

    def get_node(self, path):
        return self.fs_root.find_node(path)

    def split_parent_and_filename(self, path):
        return path.rsplit('/', 1)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True)
