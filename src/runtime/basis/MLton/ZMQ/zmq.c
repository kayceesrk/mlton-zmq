#include "platform.h"

/* Context Options */

const C_Int_t MLton_ZMQ_IO_THREADS  = ZMQ_IO_THREADS;
const C_Int_t MLton_ZMQ_MAX_SOCKETS = ZMQ_MAX_SOCKETS;

/* Sockect Types */

const C_Int_t MLton_ZMQ_REQ = ZMQ_REQ;
const C_Int_t MLton_ZMQ_REP = ZMQ_REP;
const C_Int_t MLton_ZMQ_PUB = ZMQ_PUB;
const C_Int_t MLton_ZMQ_SUB = ZMQ_SUB;
const C_Int_t MLton_ZMQ_XPUB = ZMQ_XPUB;
const C_Int_t MLton_ZMQ_XSUB = ZMQ_XSUB;
const C_Int_t MLton_ZMQ_PUSH = ZMQ_PUSH;
const C_Int_t MLton_ZMQ_PULL = ZMQ_PULL;
const C_Int_t MLton_ZMQ_PAIR = ZMQ_PAIR;
const C_Int_t MLton_ZMQ_DEALER = ZMQ_DEALER;
const C_Int_t MLton_ZMQ_ROUTER = ZMQ_ROUTER;

/* Socket Options */
const C_Int_t MLton_ZMQ_AFFINITY = ZMQ_AFFINITY;
const C_Int_t MLton_ZMQ_IDENTITY = ZMQ_IDENTITY;
const C_Int_t MLton_ZMQ_SUBSCRIBE = ZMQ_SUBSCRIBE;
const C_Int_t MLton_ZMQ_UNSUBSCRIBE = ZMQ_UNSUBSCRIBE;
const C_Int_t MLton_ZMQ_RATE = ZMQ_RATE;
const C_Int_t MLton_ZMQ_RECOVERY_IVL = ZMQ_RECOVERY_IVL;
const C_Int_t MLton_ZMQ_SNDBUF = ZMQ_SNDBUF;
const C_Int_t MLton_ZMQ_RCVBUF = ZMQ_RCVBUF;
const C_Int_t MLton_ZMQ_RCVMORE = ZMQ_RCVMORE;
const C_Int_t MLton_ZMQ_FD = ZMQ_FD;
const C_Int_t MLton_ZMQ_EVENTS = ZMQ_EVENTS;
const C_Int_t MLton_ZMQ_TYPE = ZMQ_TYPE;
const C_Int_t MLton_ZMQ_LINGER = ZMQ_LINGER;
const C_Int_t MLton_ZMQ_RECONNECT_IVL = ZMQ_RECONNECT_IVL;
const C_Int_t MLton_ZMQ_BACKLOG = ZMQ_BACKLOG;
const C_Int_t MLton_ZMQ_RECONNECT_IVL_MAX = ZMQ_RECONNECT_IVL_MAX;
const C_Int_t MLton_ZMQ_MAXMSGSIZE = ZMQ_MAXMSGSIZE;
const C_Int_t MLton_ZMQ_SNDHWM = ZMQ_SNDHWM;
const C_Int_t MLton_ZMQ_RCVHWM = ZMQ_RCVHWM;
const C_Int_t MLton_ZMQ_MULTICAST_HOPS = ZMQ_MULTICAST_HOPS;
const C_Int_t MLton_ZMQ_RCVTIMEO = ZMQ_RCVTIMEO;
const C_Int_t MLton_ZMQ_SNDTIMEO = ZMQ_SNDTIMEO;
const C_Int_t MLton_ZMQ_IPV4ONLY = ZMQ_IPV4ONLY;
const C_Int_t MLton_ZMQ_LAST_ENDPOINT = ZMQ_LAST_ENDPOINT;
const C_Int_t MLton_ZMQ_ROUTER_MANDATORY = ZMQ_ROUTER_MANDATORY;
const C_Int_t MLton_ZMQ_TCP_KEEPALIVE = ZMQ_TCP_KEEPALIVE;
const C_Int_t MLton_ZMQ_TCP_KEEPALIVE_CNT = ZMQ_TCP_KEEPALIVE_CNT;
const C_Int_t MLton_ZMQ_TCP_KEEPALIVE_IDLE = ZMQ_TCP_KEEPALIVE_IDLE;
const C_Int_t MLton_ZMQ_TCP_KEEPALIVE_INTVL = ZMQ_TCP_KEEPALIVE_INTVL;
const C_Int_t MLton_ZMQ_TCP_ACCEPT_FILTER = ZMQ_TCP_ACCEPT_FILTER;
const C_Int_t MLton_ZMQ_DELAY_ATTACH_ON_CONNECT = ZMQ_DELAY_ATTACH_ON_CONNECT;
const C_Int_t MLton_ZMQ_XPUB_VERBOSE = ZMQ_XPUB_VERBOSE;

/* Socket events */
const C_Int_t MLton_ZMQ_POLLIN = ZMQ_POLLIN;
const C_Int_t MLton_ZMQ_POLLOUT = ZMQ_POLLOUT;
const C_Int_t MLton_ZMQ_POLLERR = ZMQ_POLLERR;

/* Send/Recv options */
const C_Int_t MLton_ZMQ_DONTWAIT = ZMQ_DONTWAIT;
const C_Int_t MLton_ZMQ_SNDMORE = ZMQ_SNDMORE;


C_Errno_t(C_ZMQ_Context_t) MLton_ZMQ_ctx_new (void) {
	return (C_ZMQ_Context_t) (Pointer) zmq_ctx_new ();
}

C_Errno_t(C_Int_t) MLton_ZMQ_ctx_destroy (C_ZMQ_Context_t context) {
	return (C_Int_t) zmq_ctx_destroy ((void*)context);
}

C_Errno_t(C_Int_t) MLton_ZMQ_ctx_set (C_ZMQ_Context_t context, C_Int_t option_name, C_Int_t option_value) {
  return (C_Int_t) zmq_ctx_set ((void*)context, (int)option_name, (int)option_value);
}

C_Errno_t(C_Int_t) MLton_ZMQ_ctx_get (C_ZMQ_Context_t context, C_Int_t option_name) {
  return (C_Int_t) zmq_ctx_get((void*)context, (int)option_name);
}

C_Errno_t(C_ZMQ_Socket_t) MLton_ZMQ_socket (C_ZMQ_Context_t context, C_Int_t type) {
  return (C_ZMQ_Socket_t) (Pointer) zmq_socket ((void*)context, (int)type);
}

C_Errno_t(C_Int_t) MLton_ZMQ_close (C_ZMQ_Socket_t sock) {
  return (C_Int_t) zmq_close ((void*)sock);
}

C_Errno_t(C_Int_t) MLton_ZMQ_connect (C_ZMQ_Socket_t sock, NullString8_t endPoint) {
  return (C_Int_t) zmq_connect ((void*)sock, (const char*)endPoint);
}

C_Errno_t(C_Int_t) MLton_ZMQ_disconnect (C_ZMQ_Socket_t sock, NullString8_t endPoint) {
  return (C_Int_t) zmq_disconnect ((void*)sock, (const char*)endPoint);
}

C_Errno_t(C_Int_t) MLton_ZMQ_bind (C_ZMQ_Socket_t sock, NullString8_t endPoint) {
  return (C_Int_t) zmq_bind ((void*)sock, (const char*)endPoint);
}

C_Errno_t(C_Int_t) MLton_ZMQ_unbind (C_ZMQ_Socket_t sock, NullString8_t endPoint) {
  return (C_Int_t) zmq_unbind ((void*)sock, (const char*)endPoint);
}

C_Errno_t(C_Int_t) MLton_ZMQ_getSockOpt (C_ZMQ_Socket_t sock, C_Int_t opt,
                                         Array(Word8_t) argp, Ref(C_Size_t) optlen) {
  return zmq_getsockopt ((void*)sock, (int)opt, (void*)argp, (size_t*)optlen);
}

C_Errno_t(C_Int_t) MLton_ZMQ_setSockOpt (C_ZMQ_Socket_t sock, C_Int_t opt,
                                         Vector(Word8_t) argp, C_Size_t optlen) {
  /* For subscribe and unsubscribe, include the vector header */
  if ((opt == ZMQ_SUBSCRIBE && optlen != 0)  //Non-zero subscription
      || opt == ZMQ_UNSUBSCRIBE) { //Unsubscribe
    argp = (pointer)argp - (sizeof(void*) * 3);
    optlen += (sizeof (void*) * 3);
  }
  return zmq_setsockopt ((void*)sock, (int)opt, (void*)argp, (size_t)optlen);
}

C_Errno_t(C_ZMQ_Message_t) MLton_ZMQ_recv (C_ZMQ_Socket_t sock, C_Int_t flags) {
  zmq_msg_t *msg = (zmq_msg_t*)malloc (sizeof(zmq_msg_t));
  int rc = zmq_msg_init (msg);
  assert (rc == 0);
  rc = zmq_msg_recv (msg, (void*)sock, flags);

  if (rc == -1) {
    free (msg);
    return (C_ZMQ_Message_t)0;
  }
  return (C_ZMQ_Message_t)msg;
}
