
#pragma once

#include <thread>
#include <mutex>
#include <condition_variable>

#include "base/misc.hpp"
#include "base/threading.hpp"
#include "marshal.hpp"

namespace rrr {

class Recorder : public FrequentJob {
private:
    int fd_;
    //    string path_ = "deptran_recorder";

    Mutex mtx_;
    Timer timer_;  

//    io_context_t ctx_;
//    std::map<uint8_t*, std::function<void(void)>> callbacks_;

public:

    uint64_t batch_time_ = 10 * 1000; // 10ms

    typedef std::pair<std::string, std::function<void(void)> > io_req_t;

    std::list<io_req_t*> *flush_reqs_;
    std::list<io_req_t*> *callback_reqs_;

    Mutex mtx_cd_flush_;
    CondVar cd_flush_;
    std::thread *th_flush_;

    AvgStat stat_cnt_;
    AvgStat stat_sz_;

    Recorder(const char *path);
    Recorder(const Recorder&) = delete;
    Recorder& operator=(const Recorder&) = delete;
    //    void submit(const std::string &buf);

    void submit(const std::string &buf, 
		const std::function<void(void)> &cb = std::function<void(void)>());

    void submit(Marshal &m,
                const std::function<void(void)> &cb = std::function<void(void)>());

    void flush_buf();

    void callback(std::vector<std::pair<std::string, 
		  std::function<void(void)> > > *reqs);

    void invoke_cb();

    void flush_loop();

    void run() {
        // callback for successful flushed requests. 
        invoke_cb();

        if (timer_.elapsed() * 1000000 > batch_time_) {
        //if (true) {
            // trigger the flush thread to do the dirty work.
            cd_flush_.signal();
            timer_.start();
        }
    }

    void set_batch(uint64_t time) {
        batch_time_ = time; 
    }
    
    ~Recorder();
};

} // namespace rrr

