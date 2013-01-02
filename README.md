# mlton-zmq
=========

MLton extension for ZeroMQ (http://www.zeromq.org/). Checkout MLton.ZMQ module
@ src/basis-library/mlton/zmq.sig.

## Why do you need all of MLton's distribution for ZeroMQ binding?
===========================================================

So as to take advantage of MLton's safe system call support, such that zeromq
invocations play nicely with user-level threads with preemptive multithreading.
Since MLton's system call library is not exposed to the end user, ZMQ binding
is packaged as a module in MLton's basis library. As an added benefit, this
also leads to a concise implementation since we take advantage of safe system
call's error handling support.

## Dependencies
===============

Along with MLton's dependencies, [__libzmq__](http://www.zeromq.org/docs:core-api) needs to be installed.
