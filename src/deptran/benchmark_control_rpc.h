#ifndef BENCHMARK_CTRL_H_
#define BENCHMARK_CTRL_H_

#include "rcc/dep_graph.h"
#include "rcc_rpc.h" // before this one include all the custom data structures.

#include <time.h>
#include <sys/time.h>
#ifdef __APPLE__ // for OS X
#include <mach/clock.h>
#include <mach/mach.h>
#endif

namespace janus {

extern const char S_RES_KEY_N_SCC[];
extern const char S_RES_KEY_N_ASK[];
extern const char S_RES_KEY_START_GRAPH[];
extern const char S_RES_KEY_COMMIT_GRAPH[];
extern const char S_RES_KEY_ASK_GRAPH[];
//extern const char S_RES_KEY_CPU[];

void clock_gettime(struct timespec *time);
double timespec2ms(struct timespec time);
bool operator<(const struct timespec &lhs, const struct timespec &rhs);

class ServerControlServiceImpl: public ServerControlService {
 public:

  static const std::string STAT_SZ_SCC;
  static const std::string STAT_N_ASK;
  static const std::string STAT_SZ_GRAPH_START;
  static const std::string STAT_SZ_GRAPH_COMMIT;
  static const std::string STAT_SZ_GRAPH_ASK;
  static const std::string STAT_RO6_SZ_VECTOR;

 private:
  Recorder *recorder_;

  typedef enum {
    SCS_INIT,
    SCS_RUN,
    SCS_STOP,
  } status_t;

  //pthread_mutex_t stat_m_;
  Mutex stat_m_;
  std::unordered_map<const char *, ValueTimesPair> statistics_;
  std::map<std::string, AvgStat *> stats_;

  //pthread_mutex_t status_mutex_;
  Mutex status_mutex_;
  //pthread_cond_t status_cond_;
  CondVar status_cond_;
  status_t status_;
  unsigned int timeout_;
  bool sig_handler_set_;

  static vector<ServerControlServiceImpl*> scsi_s;

  static void shutdown_wrapper(int sig);

  void set_sig_handler();

 public:
  void server_shutdown(DeferredReply*) override;
  void server_ready(i32 *res, DeferredReply*) override;
  void server_heart_beat_with_data(ServerResponse *res, DeferredReply*) override;
  void server_heart_beat(DeferredReply*) override;

  ServerControlServiceImpl(unsigned int timeout = 5, Recorder *recorder = NULL);
  ~ServerControlServiceImpl();
  void set_ready();
  void wait_for_shutdown();

  void do_statistics(const char *key, int64_t value_delta);

  // TODO to replace this with get_stat
  void set_recorder(Recorder *recorder) {
    recorder_ = recorder;
  }

  AvgStat *get_stat(std::string str) {
    auto &stat = stats_[str];
    if (stat == NULL) {
      stat = new AvgStat();
    }
    return stat;
  }

  void set_stat(const std::string str, AvgStat *stat) {
    stats_[str] = stat;
  }

};

class ClientControlServiceImpl: public ClientControlService {
 private:
  typedef enum {
    LCS_LAST_PERIOD,
    LCS_THIS_PERIOD,
    LCS_IGNORE
  } latency_collection_status_t;

  typedef enum {
    CCS_INIT,
    CCS_READY,
    CCS_RUN,
    CCS_FINISH,
    CCS_STOP,
  } status_t;

  typedef struct txn_info_t {
    int32_t txn_type;
    int32_t commit_txn;
    int32_t start_txn;
    int32_t total_txn;
    int32_t total_try;
    int32_t retries_exhausted;
    std::vector<double> *interval_latencies[2];
    std::vector<double> *last_interval_latencies[2];
    std::vector<double> *interval_attempt_latencies[2];
    std::vector<int32_t> *num_try[2];
    std::vector<double> interval_latency;

    txn_info_t() {
      txn_type = -1;
      start_txn = 0;
      commit_txn = 0;
      total_txn = 0;
      total_try = 0;
      retries_exhausted = 0;
      num_try[0] = new std::vector<int32_t>();
      num_try[1] = new std::vector<int32_t>();

      interval_latencies[0] = new std::vector<double>();
      interval_latencies[1] = new std::vector<double>();

      last_interval_latencies[0] = new std::vector<double>();
      last_interval_latencies[1] = new std::vector<double>();

      interval_attempt_latencies[0] = new std::vector<double>();
      interval_attempt_latencies[1] = new std::vector<double>();
    }

    void init(int32_t _txn_type) {
      txn_type = _txn_type;
    }

    void destroy() {
      delete interval_latencies[0];
      delete interval_latencies[1];
      delete last_interval_latencies[0];
      delete last_interval_latencies[1];
      delete interval_attempt_latencies[0];
      delete interval_attempt_latencies[1];
      delete num_try[0];
      delete num_try[1];
    }

    void start(bool switzh) {
      start_txn++;
    }

    void give_up() {
      retries_exhausted++;
    }

    void retry(bool switzh, double attempt_latency) {
      total_try++;
      if (switzh)
        interval_attempt_latencies[0]->push_back(attempt_latency);
      else
        interval_attempt_latencies[1]->push_back(attempt_latency);
    }

    void succ(bool switzh, latency_collection_status_t lcs, double latency, double attempt_latency, int32_t tried) {
      total_txn++;
      total_try++;
      commit_txn++;
      int use = 1;
      if (switzh)
        use = 0;
      num_try[use]->push_back(tried);
      switch (lcs) {
        case LCS_THIS_PERIOD:
          interval_latencies[use]->push_back(latency);
          break;
        case LCS_LAST_PERIOD:
          last_interval_latencies[use]->push_back(latency);
          break;
        case LCS_IGNORE:
        default:
          break;
      }
      interval_attempt_latencies[use]->push_back(attempt_latency);
      interval_latency.push_back(latency);
    }

    void rej(bool switzh, latency_collection_status_t lcs, double latency, double attempt_latency, int32_t tried) {
      total_txn++;
      total_try++;
      if (switzh)
        interval_attempt_latencies[0]->push_back(attempt_latency);
      else
        interval_attempt_latencies[1]->push_back(attempt_latency);
    }
  } txn_info_t;

  std::vector<DeferredReply *> ready_block_defers_;
  //pthread_mutex_t status_mutex_;
  Mutex status_mutex_;
  //pthread_cond_t status_cond_;
  CondVar status_cond_;
  status_t status_;
  pthread_t **coo_threads_;
  std::map<int32_t, txn_info_t>* txn_info_;
  bool txn_info_switch_;

  std::recursive_mutex mtx_ = {};
  pthread_rwlock_t collect_lock_;

  unsigned int num_threads_;
  unsigned int num_ready_;
  unsigned int num_finish_;
  struct timespec start_time_;
  struct timespec last_time_, before_last_time_;

  std::map<int32_t, std::string> txn_names_;

  void LogClientResponse(ClientResponse *res);
 public:
  void client_get_txn_names(std::map<i32, std::string> *txn_names, DeferredReply*) override;
  void client_shutdown(DeferredReply*) override;
  void client_force_stop(DeferredReply*) override;
  void client_response(ClientResponse *res, DeferredReply*) override;
  void client_ready_block(i32 *res,
                          DeferredReply *defer) override;
  void client_ready(i32 *res, DeferredReply*) override;
  void client_start(DeferredReply*) override;

  ClientControlServiceImpl(unsigned int num_threads, const std::map<int32_t, std::string> &txn_types);
  ~ClientControlServiceImpl();
  void wait_for_start(unsigned int id);
  void wait_for_shutdown();

  inline void txn_give_up_one(txnid_t id, int32_t txn_type) {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
    pthread_rwlock_rdlock(&collect_lock_);
    verify(id >= 0 && id < num_threads_);
    verify(txn_info_[id].find(txn_type) != txn_info_[id].end());
    txn_info_[id][txn_type].give_up();
    pthread_rwlock_unlock(&collect_lock_);
  }

  inline void txn_start_one(unsigned int id, int32_t txn_type) {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
    pthread_rwlock_rdlock(&collect_lock_);
    verify(id >= 0 && id < num_threads_);
    verify(txn_info_[id].find(txn_type) != txn_info_[id].end());
    txn_info_[id][txn_type].start(txn_info_switch_);
    pthread_rwlock_unlock(&collect_lock_);
  }

  inline void txn_retry_one(unsigned int id, int32_t txn_type, double attempt_latency) {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
    pthread_rwlock_rdlock(&collect_lock_);
    txn_info_[id][txn_type].retry(txn_info_switch_, attempt_latency);
    pthread_rwlock_unlock(&collect_lock_);
  }

  inline void txn_success_one(unsigned int id,
                              int32_t txn_type,
                              struct timespec start_time,
                              double latency,
                              double attempt_latency,
                              int32_t tried) {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
    pthread_rwlock_rdlock(&collect_lock_);
    latency_collection_status_t lcs = LCS_IGNORE;
    if (last_time_ < start_time)
      lcs = LCS_THIS_PERIOD;
    else if (before_last_time_ < start_time)
      lcs = LCS_LAST_PERIOD;
    txn_info_[id][txn_type].succ(txn_info_switch_, lcs, latency, attempt_latency, tried);
    pthread_rwlock_unlock(&collect_lock_);
  }

  inline void txn_reject_one(unsigned int id,
                             int32_t txn_type,
                             struct timespec start_time,
                             double latency,
                             double attempt_latency,
                             int32_t tried) {
    std::lock_guard<std::recursive_mutex> guard(mtx_);
    pthread_rwlock_rdlock(&collect_lock_);
    latency_collection_status_t lcs = LCS_IGNORE;
    if (last_time_ < start_time)
      lcs = LCS_THIS_PERIOD;
    else if (before_last_time_ < start_time)
      lcs = LCS_LAST_PERIOD;
    txn_info_[id][txn_type].rej(txn_info_switch_, lcs, latency, attempt_latency, tried);
    pthread_rwlock_unlock(&collect_lock_);
  }

  void DispatchTxn(const TxDispatchRequest& req, TxReply* txn_reply, rrr::DeferredReply* defer) override;
};

}

#endif
