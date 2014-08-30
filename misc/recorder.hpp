
#pragma once

#include <thread>
#include <mutex>
#include <condition_variable>

#include "base/misc.hpp"
#include "marshal.hpp"

namespace rrr {

class Recorder : public FrequentJob {
private:
    int fd_;
    //    string path_ = "deptran_recorder";

    std::mutex mtx_;


//    io_context_t ctx_;
//    std::map<uint8_t*, std::function<void(void)>> callbacks_;

public:
    typedef std::pair<std::string, std::function<void(void)> > io_req_t;

    std::list<io_req_t*> *flush_reqs_;
    std::list<io_req_t*> *callback_reqs_;

    std::mutex mtx_cd_flush_;
    std::condition_variable cd_flush_;
    std::thread *th_flush_;

    AvgStat stat_cnt_;
    AvgStat stat_sz_;

    Recorder(const char *path);

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
	invoke_cb();
        //	flush_buf();
        cd_flush_.notify_one();        
    }

    ~Recorder();
};

} // namespace rrr

