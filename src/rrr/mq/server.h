

#include <apr_thread_pool.h>
#include "polling.h"
#include "buf.h"

typedef struct {
rpc_comm_t *comm;
    poll_job_t *pjob;
    mpr_hash_t *ht_conn;
    bool is_start;
    // thread pool for handling slow messages.
    apr_thread_pool_t *tp;
} server_t;

typedef struct {
    rpc_comm_t *comm;
    poll_job_t *pjob;
    buf_t *buf_recv;
    buf_t *buf_send;
    apr_thread_pool_t *tp;
} sconn_t;

typedef struct {
    uint8_t *raw_input;
    size_t sz_input;
    uint8_t *raw_output;
    size_t sz_output;
    sconn_t *sconn;
    msgid_t msgid;
} rpc_state_t;

typedef rpc_state_t rpc_state;

void server_create(server_t **svr, poll_mgr_t *mgr);

void server_destroy(server_t *svr);

void server_start(server_t *svr);

void server_stop(server_t *svr);

void server_reg(server_t *svr, msgid_t msgid, void* fun);

void sconn_create(sconn_t **sconn, server_t *svr);

void sconn_destroy(sconn_t *sconn);

void handle_server_accept(void* arg);

void handle_sconn_read(void* arg);

void handle_sconn_write(void* arg);

void write_trigger_poll(rpc_comm_t *comm, 
			poll_job_t* pjob, 
			buf_t *buf, 
			msgid_t msgid, 
			const uint8_t *data, 
			size_t sz_data);

void reply_to(rpc_state_t *state);
