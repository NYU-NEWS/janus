#pragma once

#include "__dep__.h"

namespace rococo {

class TxnRequest;
class Sharding;
class Config;

class TxnGenerator {
 public:
  typedef struct {
    int n_branch_;
    int n_teller_;
    int n_customer_;
  } tpca_para_t;


  typedef struct {
    int n_table_;
  } rw_benchmark_para_t;

  typedef struct {
    int n_table_a_;
    int n_table_b_;
    int n_table_c_;
    int n_table_d_;
  } micro_bench_para_t;

  union {
    tpca_para_t tpca_para_;
    rw_benchmark_para_t rw_benchmark_para_;
    micro_bench_para_t micro_bench_para_;
  };

  int benchmark_;
  int n_try_;
  int single_server_;
  int fix_id_;
  std::vector<double>& txn_weight_;
  std::map<string, double>& txn_weights_;
  Sharding* sharding_;

 public:
  TxnGenerator(Config* config);

  virtual void get_txn_req(TxnRequest *req, uint32_t cid) const;

  // rw_benchmark
  void get_rw_benchmark_txn_req(TxnRequest *req, uint32_t cid) const;
  void get_rw_benchmark_w_txn_req(TxnRequest *req, uint32_t cid) const;
  void get_rw_benchmark_r_txn_req(TxnRequest *req, uint32_t cid) const;

  // tpca
  void get_tpca_txn_req(TxnRequest *req, uint32_t cid) const;

  // tpcc

  void get_micro_bench_read_req(TxnRequest *req, uint32_t cid) const;
  void get_micro_bench_write_req(TxnRequest *req, uint32_t cid) const;
  void get_micro_bench_txn_req(TxnRequest *req, uint32_t cid) const;
  void get_txn_types(std::map<int32_t, std::string> &txn_types);

  ~TxnGenerator();
};

} // namespace rcc

