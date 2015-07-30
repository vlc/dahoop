Setup
----

 * GHC 7.10.x
 * ZeroMQ library
   - http://download.zeromq.org/zeromq-4.0.7.tar.gz

Compiling ZeromMQ
---

To produce libzmq.dll

Using msys2, the hand building was okay:

# get src, unpack, cd zeromq-4.0.7
export CPPFLAGS=-DFD_SETSIZE=1024
export CFLAGS=-DFD_SETSIZE=1024
./configure && make
# get the dll out of src/.libs