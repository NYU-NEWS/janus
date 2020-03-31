#include <string>
#include <memory>

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/tcp.h>

#include "reactor/coroutine.h"
#include "client.hpp"
#include "utils.hpp"

using namespace std;

namespace rrr {

void Future::wait() {
  Pthread_mutex_lock(&ready_m_);
  while (!ready_ && !timed_out_) {
    Pthread_cond_wait(&ready_cond_, &ready_m_);
  }
  Pthread_mutex_unlock(&ready_m_);
}

void Future::timed_wait(double sec) {
  Pthread_mutex_lock(&ready_m_);
  while (!ready_ && !timed_out_) {
    int full_sec = (int) sec;
    int nsec = int((sec - full_sec) * 1000 * 1000 * 1000);
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    timespec abstime;
    abstime.tv_sec = tv.tv_sec + full_sec;
    abstime.tv_nsec = tv.tv_usec * 1000 + nsec;
    if (abstime.tv_nsec > 1000 * 1000 * 1000) {
      abstime.tv_nsec -= 1000 * 1000 * 1000;
      abstime.tv_sec += 1;
    }
    Log::debug("wait for %lf", sec);
    int ret = pthread_cond_timedwait(&ready_cond_, &ready_m_, &abstime);
    if (ret == ETIMEDOUT) {
      timed_out_ = true;
    } else {
      verify(ret == 0);
    }
  }
  Pthread_mutex_unlock(&ready_m_);
  if (timed_out_) {
    error_code_ = ETIMEDOUT;
    if (attr_.callback != nullptr) {
      attr_.callback(this);
    }
  }
}

void Future::notify_ready() {
  Pthread_mutex_lock(&ready_m_);
  if (!timed_out_) {
    ready_ = true;
  }
  Pthread_cond_signal(&ready_cond_);
  Pthread_mutex_unlock(&ready_m_);
  if (ready_ && attr_.callback != nullptr) {
    // Warning: make sure memory is safe!
    auto x = attr_.callback;
    Coroutine::CreateRun([x, this]() {
      x(this);
    });
//        attr_.callback(this);
  }
}

void Client::invalidate_pending_futures() {
  list<Future*> futures;
  pending_fu_l_.lock();
  for (auto& it: pending_fu_) {
    futures.push_back(it.second);
  }
  pending_fu_.clear();
  pending_fu_l_.unlock();

  for (auto& fu: futures) {
    if (fu != nullptr) {
      fu->error_code_ = ENOTCONN;
      fu->notify_ready();

      // since we removed it from pending_fu_
      fu->release();
    }
  }
}

void Client::close() {
  if (status_ == CONNECTED) {
    pollmgr_->remove(shared_from_this());
    ::close(sock_);
  }
  status_ = CLOSED;
  invalidate_pending_futures();
}

int Client::connect(const char* addr) {
  verify(status_ != CONNECTED);
  string addr_str(addr);
  size_t idx = addr_str.find(":");
  if (idx == string::npos) {
    Log_error("rrr::Client: bad connect address: %s", addr);
    return EINVAL;
  }
  string host = addr_str.substr(0, idx);
  string port = addr_str.substr(idx + 1);
#ifdef USE_IPC
  struct sockaddr_un saun;
  saun.sun_family = AF_UNIX;
  string ipc_addr = "rsock" + port;
  strcpy(saun.sun_path, ipc_addr.data());
  if ((sock_ = socket(AF_UNIX, SOCK_STREAM, 0)) < 0) {
    perror("client: socket");
    exit(1);
  }
  auto len = sizeof(saun.sun_family) + strlen(saun.sun_path)+1;
  if (::connect(sock_, (struct sockaddr*)&saun, len) < 0) {
    perror("client: connect");
    exit(1);
  }
#else

  struct addrinfo hints, * result, * rp;
  memset(&hints, 0, sizeof(struct addrinfo));

  hints.ai_family = AF_INET; // ipv4
  hints.ai_socktype = SOCK_STREAM; // tcp

  int r = getaddrinfo(host.c_str(), port.c_str(), &hints, &result);
  if (r != 0) {
    Log_error("rrr::Client: getaddrinfo(): %s", gai_strerror(r));
    return EINVAL;
  }

  for (rp = result; rp != nullptr; rp = rp->ai_next) {
    sock_ = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
    if (sock_ == -1) {
      continue;
    }

    const int yes = 1;
    verify(setsockopt(sock_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) == 0);
    verify(setsockopt(sock_, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == 0);
    int buf_len = 1024 * 1024;
    setsockopt(sock_, SOL_SOCKET, SO_RCVBUF, &buf_len, sizeof(buf_len));
    setsockopt(sock_, SOL_SOCKET, SO_SNDBUF, &buf_len, sizeof(buf_len));

    if (::connect(sock_, rp->ai_addr, rp->ai_addrlen) == 0) {
      break;
    }
    ::close(sock_);
    sock_ = -1;
  }
  freeaddrinfo(result);

  if (rp == nullptr) {
    // failed to connect
    // Log_error("rrr::Client: connect(%s): %s", addr, strerror(errno));
    return ENOTCONN;
  }
#endif
  verify(set_nonblocking(sock_, true) == 0);
  Log_debug("rrr::Client: connected to %s", addr);

  status_ = CONNECTED;
  pollmgr_->add(shared_from_this());

  return 0;
}

void Client::handle_error() {
  close();
}

void Client::handle_write() {
  if (status_ != CONNECTED) {
    return;
  }

  out_l_.lock();
  out_.write_to_fd(sock_);

  if (out_.empty()) {
    pollmgr_->update_mode(shared_from_this(), Pollable::READ);
  }
  out_l_.unlock();
}

void Client::handle_read() {
  if (status_ != CONNECTED) {
    return;
  }

  int bytes_read = in_.read_from_fd(sock_);
  if (bytes_read == 0) {
    return;
  }

  for (;;) {
    i32 packet_size;
    int n_peek = in_.peek(&packet_size, sizeof(i32));
    if (n_peek == sizeof(i32)
        && in_.content_size() >= packet_size + sizeof(i32)) {
      // consume the packet size
      verify(in_.read(&packet_size, sizeof(i32)) == sizeof(i32));

      v64 v_reply_xid;
      v32 v_error_code;

      in_ >> v_reply_xid >> v_error_code;

      pending_fu_l_.lock();
      unordered_map<i64, Future*>::iterator
          it = pending_fu_.find(v_reply_xid.get());
      if (it != pending_fu_.end()) {
        Future* fu = it->second;
        verify(fu->xid_ == v_reply_xid.get());
        pending_fu_.erase(it);
        pending_fu_l_.unlock();

        fu->error_code_ = v_error_code.get();
        fu->reply_.read_from_marshal(in_,
                                     packet_size - v_reply_xid.val_size()
                                         - v_error_code.val_size());

        fu->notify_ready();

        // since we removed it from pending_fu_
        fu->release();
      } else {
        // the future might timed out
        pending_fu_l_.unlock();
      }

    } else {
      // packet incomplete or no more packets to process
      break;
    }
  }
  // This is a workaround, the Loop call should really happen
  // between handle_read and handle_write in the epoll loop
  Reactor::GetReactor()->Loop();
}

int Client::poll_mode() {
  int mode = Pollable::READ;
  out_l_.lock();
  if (!out_.empty()) {
    mode |= Pollable::WRITE;
  }
  out_l_.unlock();
  return mode;
}

Future* Client::begin_request(i32 rpc_id, const FutureAttr& attr /* =... */) {
  out_l_.lock();

  if (status_ != CONNECTED) {
    return nullptr;
  }

  Future* fu = new Future(xid_counter_.next(), attr);
  pending_fu_l_.lock();
  pending_fu_[fu->xid_] = fu;
  pending_fu_l_.unlock();

  // check if the client gets closed in the meantime
  if (status_ != CONNECTED) {
    pending_fu_l_.lock();
    unordered_map<i64, Future*>::iterator it = pending_fu_.find(fu->xid_);
    if (it != pending_fu_.end()) {
      it->second->release();
      pending_fu_.erase(it);
    }
    pending_fu_l_.unlock();

    return nullptr;
  }

  bmark_ = out_.set_bookmark(sizeof(i32)); // will fill packet size later

  *this << v64(fu->xid_);
  *this << rpc_id;

  // one ref is already in pending_fu_
  return (Future*) fu->ref_copy();
}

void Client::end_request() {
  // set reply size in packet
  if (bmark_ != nullptr) {
    i32 request_size = out_.get_and_reset_write_cnt();
    out_.write_bookmark(bmark_, &request_size);
    delete bmark_;
    bmark_ = nullptr;
  }

  // always enable write events since the code above gauranteed there
  // will be some data to send
  pollmgr_->update_mode(shared_from_this(), Pollable::READ | Pollable::WRITE);

  out_l_.unlock();
}

ClientPool::ClientPool(PollMgr* pollmgr /* =? */,
                       int parallel_connections /* =? */)
    : parallel_connections_(parallel_connections) {

  verify(parallel_connections_ > 0);
  if (pollmgr == nullptr) {
    pollmgr_ = new PollMgr;
  } else {
    pollmgr_ = (PollMgr*) pollmgr->ref_copy();
  }
}

ClientPool::~ClientPool() {
  for (auto& it : cache_) {
    for (int i = 0; i < parallel_connections_; i++) {
      it.second[i]->close_and_release();
    }
    delete[] it.second;
  }
  pollmgr_->release();
}

Client* ClientPool::get_client(const string& addr) {
  Client* cl = nullptr;
  l_.lock();
  map<string, Client**>::iterator it = cache_.find(addr);
  if (it != cache_.end()) {
    cl = it->second[rand_() % parallel_connections_];
  } else {
    Client** parallel_clients = new Client* [parallel_connections_];
    int i;
    bool ok = true;
    for (i = 0; i < parallel_connections_; i++) {
      parallel_clients[i] = new Client(this->pollmgr_);
      if (parallel_clients[i]->connect(addr.c_str()) != 0) {
        ok = false;
        break;
      }
    }
    if (ok) {
      cl = parallel_clients[rand_() % parallel_connections_];
      insert_into_map(cache_, addr, parallel_clients);
    } else {
      // close connections
      while (i >= 0) {
        parallel_clients[i]->close_and_release();
        i--;
      }
      delete[] parallel_clients;
    }
  }
  l_.unlock();
  return cl;
}

} // namespace rrr
