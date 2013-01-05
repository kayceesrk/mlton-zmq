# Distributed MLton
================================

MLton support for distribution through for ZeroMQ (http://www.zeromq.org/). A
key feature of this implementation being automatic marshalling of arbitrary ML 
datatypes, including functions (with caveats, of course). As a result, the basic
send and receive functions are polymorphic, and are simply:

	val send : socket * 'a -> unit
	val recv : socket -> 'a

Checkout MLton.ZMQ module @ src/basis-library/mlton/zmq.sig for more information.

## Dependencies
===============

Along with MLton's dependencies,
[__libzmq__](http://www.zeromq.org/docs:core-api) needs to be installed.


## Notes
========

### Serialization support and caveats
====================================

Serialization support is achieved through two new functions under MLton structure:

	val serialize   : 'a -> Word8.word vector
	val deserialize : Word8.word -> 'a

The serilization support is very usable, but it has its caveats:

* Serialization and deserialization can only be done by the same exact program.
	In other words, if the serialized object is sent over the network, the
	receiver program must be the same as the one which serialized the object.
	Since MLton object headers are unique for each program, this restriction is
	in place.
* Use of serialization breaks static type-checking guarantees. Deserialize
	function's return type is determined by the type to which the result is
	unified. The onus is on the programmer to get this type the same as the
	original object type.
* Serialization copies mutable ref cells and arrays.
* Serialization of objects containing references to file handles, sockets are
	bound to break on the deserialization side. (Note to self, such instances
	must be statically prevented).


### Why do you need all of MLton's distribution for ZeroMQ binding?
=================================================================

So as to take advantage of MLton's safe system call support, such that zeromq
invocations play nicely with user-level threads with preemptive multithreading.
Since MLton's system call library is not exposed to the end user, ZMQ binding
is packaged as a module in MLton's basis library. As an added benefit, this
also leads to a concise implementation since we take advantage of safe system
call's error handling support.
