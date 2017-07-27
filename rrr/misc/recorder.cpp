/*
 *
 * Here is how it works, there is a queue, a flush thread, and a callback thread.
 *
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include <chrono>

#include "base/logging.hpp"
#include "stat.hpp"
#include "recorder.hpp"

namespace rrr {

Recorder::Recorder(const char *path) {
    Log::debug("disk log into %s", path);

    fd_ = open(path, O_RDWR | O_CREAT, 0644);
    if (errno == EINVAL) {
	Log::error("Open record file failed, are"
		   " yo2u trying to write into a tmpfs?");
	fd_ = open(path, O_RDWR | O_CREAT, 0644);
    }
    if (fd_ <= 0) {
	Log::error("Open record file failed, errno:"
		   " %d, %s", errno, strerror(errno));
	verify(fd_ > 0);
    }

    flush_reqs_ = new std::list<io_req_t*>();
    callback_reqs_ = new std::list<io_req_t*>();


    th_flush_ = new std::thread(&Recorder::flush_loop, this);

    timer_.start();

//    th_flush_ = new std::thread([this] () {
//	    this->flush_loop();
//	});
    //    th_pool_ = new base::ThreadPool(1);
}

void Recorder::flush_loop() {
    while (true) {
        mtx_cd_flush_.lock();

        auto now = std::chrono::system_clock::now();
        cd_flush_.wait(mtx_cd_flush_);
        flush_buf();
        mtx_cd_flush_.unlock();
    }
}

//void Recorder::submit(const std::string &buf) {
//    std::function<void(void)> empty_func;
//    submit(buf, empty_func);
//}

void Recorder::submit(const std::string &buf,
		      const std::function<void(void)> &cb) {

    io_req_t *req = new io_req_t(buf, cb);
    ScopedLock(this->mtx_);
    flush_reqs_->push_back(req);

//    if (cb) {
//        cd_flush_.notify_one();
//    }
}

void Recorder::submit(Marshal &m,
                      const std::function<void(void)> &cb) {
    io_req_t *req = new io_req_t();
    std::string &s = req->first;
    req->second = cb;

    s.resize(m.content_size());
    m.write((void*)s.data(), m.content_size());

    ScopedLock(this->mtx_);
    flush_reqs_->push_back(req);
}

void Recorder::flush_buf() {
    mtx_.lock();

    int cnt_flush = 0;
    int sz_flush = 0;

    int sz = flush_reqs_->size();
    auto reqs = flush_reqs_;

    if (sz > 0) {
	flush_reqs_ = new std::list<io_req_t*>;
    }

    mtx_.unlock();

    if (sz == 0) {
	return;
    }

    for (auto &p: *reqs) {
	std::string &s = p->first;
	int ret = write(fd_, s.data(), s.size());
	verify(ret == s.size());
        cnt_flush ++;
        sz_flush += ret;
    }
#ifndef __APPLE__
    fdatasync(fd_);
#endif

    stat_cnt_.sample(cnt_flush);
    stat_sz_.sample(sz_flush);

    // push to call back reqs.

    mtx_.lock();
    callback_reqs_->insert(callback_reqs_->end(),
                           reqs->begin(), reqs->end());
    mtx_.unlock();
    return;

}

void Recorder::invoke_cb() {
    mtx_.lock();
    int sz = callback_reqs_->size();
    auto reqs = callback_reqs_;
    if (sz > 0) {
        callback_reqs_ = new std::list<io_req_t*>;
    }
    mtx_.unlock();

    if (sz == 0) {
        return;
    }

    for (auto &p: *reqs) {
        auto &cb = p->second;
        if (cb) {
            cb();
        }
        delete p;
    }
    delete reqs;
}

Recorder::~Recorder() {
}

} // namespace rrr
