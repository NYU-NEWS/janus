#include <counter.h>
#include <signal.h>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

class CounterServiceImpl : public CounterService {
private:
    unsigned int time_;
    uint64_t last_count_, count_;
    struct timespec tv_;
    static void alarm_handler(int sig) {
        struct timespec tv_buf;
        clock_gettime(CLOCK_REALTIME, &tv_buf);
        double time = (double)(tv_buf.tv_sec - csi_s->tv_.tv_sec) + ((double)(tv_buf.tv_nsec - csi_s->tv_.tv_nsec)) / 1000000000.0;
        fprintf(stdout, "time: %lf, count: %lu, rpc per sec: %lf\n", time, csi_s->count_ - csi_s->last_count_, (csi_s->count_ - csi_s->last_count_) / time);
        csi_s->last_count_ = csi_s->count_;
        csi_s->tv_.tv_sec = tv_buf.tv_sec;
        csi_s->tv_.tv_nsec = tv_buf.tv_nsec;
        alarm(csi_s->time_);
    }

    static CounterServiceImpl *csi_s;
public:
    CounterServiceImpl(unsigned int time = 0) : time_(time), last_count_(0), count_(0) {
        clock_gettime(CLOCK_REALTIME, &tv_);

        struct sigaction sact;
        sigemptyset(&sact.sa_mask);
        sact.sa_flags = 0;
        sact.sa_handler = CounterServiceImpl::alarm_handler;
        sigaction(SIGALRM, &sact, NULL);
        alarm(time);

        csi_s = this;
    }

    ~CounterServiceImpl() {
    }

    void add() {
        count_++;
    }

    void add_long(const rrr::i32& a, const rrr::i32& b, const rrr::i32& c, const rrr::i64& d, const rrr::i64& e, const std::vector<rrr::i64>& input, rrr::i32* out, std::vector<rrr::i64>* output) {
        count_++;
        output->insert(output->end(), {1, 2/*, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10*/});
    }

    void add_short(const rrr::i64& a, rrr::i32* out) {
        count_++;
    }
};

CounterServiceImpl *CounterServiceImpl::csi_s = NULL;

int main(int argc, char **argv) {
    if (argc != 3)
        return -1;
    unsigned int time = atoi(argv[2]);
    CounterServiceImpl *csi = new CounterServiceImpl(time);
    rrr::PollMgr *pm = new rrr::PollMgr(1);
    base::ThreadPool *tp = new base::ThreadPool(1);
    rrr::Server *server = new rrr::Server(pm, tp);
    server->reg(csi);
    server->start((std::string("0.0.0.0:") + argv[1]).c_str());
    pm->release();
    tp->release();

    while (1)
        sleep(10000);
    delete server;
    delete csi;
}
