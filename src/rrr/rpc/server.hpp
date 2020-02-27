#pragma once

#include <unordered_map>
#include <unordered_set>
#include <pthread.h>
#include <memory>

#include <sys/socket.h>
#include <netdb.h>

#include "misc/marshal.hpp"
#include "reactor/epoll_wrapper.h"
#include "reactor/reactor.h"

// for getaddrinfo() used in Server::start()
//struct addrinfo;

namespace rrr {

class Server;

/**
 * The raw packet sent from client will be like this:
 * <size> <xid> <rpc_id> <arg1> <arg2> ... <argN>
 * NOTE: size does not include the size itself (<xid>..<argN>).
 *
 * For the request object, the marshal only contains <arg1>..<argN>,
 * other fields are already consumed.
 */
struct Request {
    Marshal m;
    i64 xid;
};

class Service {
public:
    virtual ~Service() {}
    virtual int __reg_to__(Server*) = 0;
};

class ServerListener: public Pollable {
  friend class Server;
 public:
  std:: string addr_;
  Server* server_;
  // cannot use smart pointers for memory management because this pointer
  // needs to be freed by freeaddrinfo.
  struct addrinfo* p_gai_result_{nullptr};
  struct addrinfo* p_svr_addr_{nullptr};

  int server_sock_{0};
  int poll_mode() {
    return Pollable::READ;
  }
  void handle_write() {verify(0);}
  void handle_read();
  void handle_error() {verify(0);}
  void close();
  int fd() {return server_sock_;}
  ServerListener(Server* s, std::string addr);
//protected:
  virtual ~ServerListener() {
    if (p_gai_result_ != nullptr) {
      freeaddrinfo(p_gai_result_);
      p_gai_result_ = nullptr;
      p_svr_addr_ = nullptr;
    }
  };
};

class ServerConnection: public Pollable {

    friend class Server;

    Marshal in_, out_;
    SpinLock out_l_;

    Server* server_;
    int socket_;

    Marshal::bookmark* bmark_;

    enum {
        CONNECTED, CLOSED
    } status_;

    /**
     * Only to be called by:
     * 1: ~Server(), which is called when destroying Server
     * 2: handle_error(), which is called by PollMgr
     */
    void close();

    // used to surpress multiple "no handler for rpc_id=..." errro
    static std::unordered_set<i32> rpc_id_missing_s;
    static SpinLock rpc_id_missing_l_s;


public:
  // Protected destructor as required by RefCounted.
  ~ServerConnection();

  ServerConnection(Server* server, int socket);

  bool connected() {
    return status_ == CONNECTED;
  }

    /**
     * Start a reply message. Must be paired with end_reply().
     *
     * Reply message format:
     * <size> <xid> <error_code> <ret1> <ret2> ... <retN>
     * NOTE: size does not include size itself (<xid>..<retN>).
     *
     * User only need to fill <ret1>..<retN>.
     *
     * Currently used errno:
     * 0: everything is fine
     * ENOENT: method not found
     * EINVAL: invalid packet (field missing)
     */
    void begin_reply(Request* req, i32 error_code = 0);

    void end_reply();

    // helper function, do some work in background
    int run_async(const std::function<void()>& f);

    template<class T>
    ServerConnection& operator <<(const T& v) {
        this->out_ << v;
        return *this;
    }

    ServerConnection& operator <<(Marshal& m) {
        this->out_.read_from_marshal(m, m.content_size());
        return *this;
    }

    int fd() {
        return socket_;
    }

    int poll_mode();
    void handle_write();
    void handle_read();
    void handle_error();
};

class DeferredReply: public NoCopy {
    rrr::Request* req_;
    ServerConnection* sconn_; // cannot delete this because of legacy reasons: need to modify the rpc compiler.
    std::function<void()> marshal_reply_;
    std::function<void()> cleanup_;
//    std::weak_ptr<ServerConnection> wp_sconn_;
    std::shared_ptr<ServerConnection> sp_sconn_{};

 public:

    DeferredReply(rrr::Request* req, ServerConnection* sconn,
                  const std::function<void()>& marshal_reply, const std::function<void()>& cleanup)
        : req_(req), sconn_(sconn), marshal_reply_(marshal_reply), cleanup_(cleanup) {
      sp_sconn_ = std::dynamic_pointer_cast<ServerConnection>(sconn->shared_from_this());
      auto x = sp_sconn_;
      verify(x);
    }

    ~DeferredReply() {
        cleanup_();
        delete req_;
        req_ = nullptr;
        sp_sconn_.reset();
    }

    int run_async(const std::function<void()>& f) {
      // TODO disable threadpool run in RPCs.
//        return sconn_->run_async(f);
      return 0;
    }

    void reply() {
//      auto sconn = wp_sconn_.lock();
      verify(sp_sconn_);
      auto sconn = sp_sconn_;
      if (sconn && sconn->connected()) {
        sconn->begin_reply(req_);
        marshal_reply_();
        sconn->end_reply();
      } else {
        // server connection has close. What would happen if no reply?
      }
      // CHECK
      // BUG here, this is deleted twice????
      delete this;
    }
};

class Server: public NoCopy {
    friend class ServerConnection;
 public:
    std::unordered_map<i32, std::function<void(Request*, ServerConnection*)>> handlers_;
    PollMgr* pollmgr_;
    ThreadPool* threadpool_;
    int server_sock_;

    Counter sconns_ctr_;

    SpinLock sconns_l_;
    std::unordered_set<shared_ptr<ServerConnection>> sconns_{};
    std::shared_ptr<ServerListener> sp_server_listener_{};

    enum {
        NEW, RUNNING, STOPPING, STOPPED
    } status_;

    pthread_t loop_th_;

    static void* start_server_loop(void* arg);
    void server_loop(struct addrinfo* svr_addr);

public:
    std::string addr_;

    Server(PollMgr* pollmgr = nullptr, ThreadPool* thrpool = nullptr);
    virtual ~Server();

    int start(const char* bind_addr);

    int reg(Service* svc) {
        return svc->__reg_to__(this);
    }

    /**
     * The svc_func need to do this:
     *
     *  {
     *     // process request
     *     ..
     *
     *     // send reply
     *     server_connection->begin_reply();
     *     *server_connection << {reply_content};
     *     server_connection->end_reply();
     *
     *     // cleanup resource
     *     delete request;
     *     server_connection->release();
     *  }
     */
    int reg(i32 rpc_id, const std::function<void(Request*, ServerConnection*)>& func);

    template<class S>
    int reg(i32 rpc_id, S* svc, void (S::*svc_func)(Request*, ServerConnection*)) {

        // disallow duplicate rpc_id
        if (handlers_.find(rpc_id) != handlers_.end()) {
            return EEXIST;
        }

        handlers_[rpc_id] = [svc, svc_func] (Request* req, ServerConnection* sconn) {
            (svc->*svc_func)(req, sconn);
        };

        return 0;
    }

    void unreg(i32 rpc_id);
};

} // namespace rrr

