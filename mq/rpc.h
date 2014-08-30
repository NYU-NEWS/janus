#ifndef RPC_H
#define RPC_H

#include <netinet/in.h>
#include <apr_thread_proc.h>
#include <apr_thread_pool.h>
#include <apr_poll.h>
#include "utils/mpr_hash.h"

#include "rpc_comm.h"
#include "buf.h"
#include "polling.h"
#include "server.h"
#include "client.h"

void rpc_init();

void rpc_destroy();

#endif
