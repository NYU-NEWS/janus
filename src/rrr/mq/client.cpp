
#include <apr_time.h>
#include "polling.h"
#include "rpc_comm.h"
#include "server.h"
#include "client.h"

extern poll_mgr_t *mgr_;

void client_create(client_t **cli, poll_mgr_t *mgr) {
    *cli = (client_t *) malloc(sizeof(client_t));
    client_t *c = *cli;
    rpc_common_create(&c->comm);
    poll_job_create(&c->pjob);
    c->pjob->do_read = handle_client_read;
    c->pjob->do_write = handle_client_write;
    c->pjob->holder = c;
    c->pjob->mgr = (mgr != NULL) ? mgr : mgr_;

    buf_create(&c->buf_recv);
    buf_create(&c->buf_send);
    LOG_DEBUG("a new client created.");
}

void client_destroy(client_t *cli) {
    rpc_common_destroy(cli->comm);
    buf_destroy(cli->buf_recv);
    buf_destroy(cli->buf_send);
    free(cli);
    LOG_DEBUG("a client destroyed");
}

void client_connect(client_t *cli) {
    LOG_DEBUG("connecting to server %s %d", cli->comm->ip, cli->comm->port);
    
    apr_status_t status = APR_SUCCESS;
    status = apr_sockaddr_info_get(&cli->comm->sa, cli->comm->ip, APR_INET, 
				   cli->comm->port, 0, cli->comm->mp);
    SAFE_ASSERT(status == APR_SUCCESS);

    status = apr_socket_create(&cli->comm->s, cli->comm->sa->family, 
			       SOCK_STREAM, APR_PROTO_TCP, cli->comm->mp);
    SAFE_ASSERT(status == APR_SUCCESS);

    status = apr_socket_opt_set(cli->comm->s, APR_TCP_NODELAY, 1);
    SAFE_ASSERT(status == APR_SUCCESS);

    while (1) {
	// repeatedly retry
        LOG_TRACE("TCP CLIENT TRYING TO CONNECT.");
        status = apr_socket_connect(cli->comm->s, cli->comm->sa);
        if (status == APR_SUCCESS /*|| status == APR_EINPROGRESS */) {
            break;
        } else if (status == APR_ECONNREFUSED) {
	    LOG_DEBUG("client connect refused, maybe the server is not ready yet");
	} else if (status == APR_EINVAL) {
	    LOG_ERROR("client connect error, invalid argument. ip: %s, port: %d", 
		      cli->comm->ip, cli->comm->port);
	    SAFE_ASSERT(0);
	} else {
            LOG_ERROR("client connect error:%s", apr_strerror(status, (char*)malloc(100), 100));
	    SAFE_ASSERT(0);
        }
	apr_sleep(50 * 1000);
    }
    LOG_INFO("connected socket on remote addr %s, port %d", cli->comm->ip, cli->comm->port);
    status = apr_socket_opt_set(cli->comm->s, APR_SO_NONBLOCK, 1);
    SAFE_ASSERT(status == APR_SUCCESS);
    
    // add to epoll
//    context_t *ctx = c->ctx;
//    while (ctx->ps == NULL) {
//        // not inited yet, just wait.
//    }
//
    apr_pollfd_t pfd = {cli->comm->mp, APR_POLL_SOCKET, APR_POLLIN, 0, {NULL}, NULL};
    pfd.desc.s = cli->comm->s;
    pfd.client_data = cli->pjob;
    cli->pjob->pfd = pfd;
    poll_mgr_add_job(cli->pjob->mgr, cli->pjob);
    
   // status = apr_pollset_add(pollset_, &ctx->pfd);
    //  status = apr_pollset_add(ctx->ps, &ctx->pfd);
    //    SAFE_ASSERT(status == APR_SUCCESS);
}

void client_disconnect(client_t *cli) {

}

void handle_client_read(void *arg) {
    client_t *cli = (client_t*) arg;
    buf_t *buf = cli->buf_recv;
    apr_socket_t *sock = cli->pjob->pfd.desc.s;

    apr_status_t status = buf_from_sock(buf, sock);

    // invoke msg handling.
    size_t sz_c = 0;
    while ((sz_c = buf_sz_content(buf)) > SZ_SZMSG) {
	uint32_t sz_msg = 0;
	buf_peek(buf, (uint8_t*)&sz_msg, sizeof(sz_msg));
	if (sz_c >= sz_msg + SZ_SZMSG + SZ_MSGID) {
	    buf_read(buf, (uint8_t*)&sz_msg, SZ_SZMSG);
	    msgid_t msgid = 0;
	    buf_read(buf, (uint8_t*)&msgid, SZ_MSGID);

	    rpc_state *state = (rpc_state*)malloc(sizeof(rpc_state));
            state->sz_input = sz_msg;
            state->raw_input = (uint8_t *)malloc(sz_msg);
	    //            state->ctx = ctx;

	    buf_read(buf, state->raw_input, sz_msg);
//            state->ctx = ctx;
/*
            apr_thread_pool_push(tp_on_read_, (*(ctx->on_recv)), (void*)state, 0, NULL);
//            mpr_thread_pool_push(tp_read_, (void*)state);
*/
//            apr_atomic_inc32(&n_data_recv_);
            //(*(ctx->on_recv))(NULL, state);
            // FIXME call

            rpc_state* (**fun)(void*) = NULL;
            size_t sz;
            mpr_hash_get(cli->comm->ht, &msgid, SZ_MSGID, (void**)&fun, &sz);
            SAFE_ASSERT(fun != NULL);
            LOG_TRACE("going to call function %x", *fun);
	    //            ctx->n_rpc++;
	    //            ctx->sz_recv += n;
            (**fun)(state);

            free(state->raw_input);
            free(state);
	} else {
	    break;
	}
    }

    if (status == APR_SUCCESS) {
	
    } else if (status == APR_EOF) {
        LOG_DEBUG("cli poll on read, received eof, close socket");
	poll_mgr_remove_job(cli->pjob->mgr, cli->pjob);
    } else if (status == APR_ECONNRESET) {
        LOG_ERROR("cli poll on read. connection reset.");
	poll_mgr_remove_job(cli->pjob->mgr, cli->pjob);
        // TODO [improve] you may retry connect
    } else if (status == APR_EAGAIN) {
        LOG_DEBUG("cli poll on read. read socket busy, resource temporarily unavailable.");
        // do nothing.
    } else {
        LOG_ERROR("unkown error on poll reading. %s\n", 
		  apr_strerror(status, (char*)malloc(100), 100));
        SAFE_ASSERT(0);
    }
}

void handle_client_write(void *arg) {
    client_t *cli = (client_t*) arg;
    buf_t *buf = cli->buf_send;
    apr_socket_t *sock = cli->pjob->pfd.desc.s;

    LOG_TRACE("handle client write");

    apr_thread_mutex_lock(cli->comm->mx);
    apr_status_t status = buf_to_sock(buf, sock);

    SAFE_ASSERT(status == APR_SUCCESS || status == APR_EAGAIN);

    if (status == APR_SUCCESS || status == APR_EAGAIN) {
	int mode = (buf_sz_cnt(buf) > 0) ? (APR_POLLIN | APR_POLLOUT) : APR_POLLIN;
	if (buf_sz_cnt(buf) == 0) {
	    poll_mgr_update_job(cli->pjob->mgr, cli->pjob, mode);
	}

	if (status == APR_EAGAIN) {
	    LOG_DEBUG("cli poll on write, socket busy, resource "
		      "temporarily unavailable. mode: %s, size in send buf: %d", 
		      mode == APR_POLLIN ? "only in" : "in and out",
		      buf_sz_cnt(buf));
	} else {
	    LOG_DEBUG("cli write something out.");
	}
    } else if (status == APR_ECONNRESET) {
        LOG_ERROR("connection reset on write, is this a mac os?");
	poll_mgr_remove_job(cli->pjob->mgr, cli->pjob);
        return;
    } else if (status == APR_EAGAIN) {
	SAFE_ASSERT(0);
    } else if (status == APR_EPIPE) {
        LOG_ERROR("on write, broken pipe, epipe error, is this a mac os?");
	poll_mgr_remove_job(cli->pjob->mgr, cli->pjob);
        return;
    } else {
        LOG_ERROR("error code: %d, error message: %s",
		  (int)status, apr_strerror(status, (char*)malloc(100), 100));
        SAFE_ASSERT(status == APR_SUCCESS);
    }

    //    poll_mgr_update_job(cli->pjob->mgr, cli->pjob, mode);
    apr_thread_mutex_unlock(cli->comm->mx);
}

void client_reg(client_t *cli, msgid_t msgid, void* fun) {
    LOG_TRACE("client regisger function, %x", fun);
    mpr_hash_set(cli->comm->ht, &msgid, SZ_MSGID, &fun, sizeof(void*)); 
}

void client_call(client_t *cli, 
		 msgid_t msgid, 
		 const uint8_t *data, 
		 size_t sz_data) {
    write_trigger_poll(cli->comm,
		       cli->pjob,
		       cli->buf_send,
		       msgid,
		       data,
		       sz_data);
}

