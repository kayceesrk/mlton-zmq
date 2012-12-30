mlton-zmq
=========

MLton extension for ZeroMQ (http://www.zeromq.org/). Checkout MLton.ZMQ module.

Why do you need the entire compiler for ZeroMQ binding?
======================================================

So as to take advantage of MLton's safe system call support, such that zeromq invovations play nicely with user-level threads with preemptive multithreading. Since MLton's system call library is not exposed to the end user, ZMQ binding is packaged as a module in MLton's basis library. As an added benefit, this also leads to a concise implementation since we take advantage of safe system call's error handling support.