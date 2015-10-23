#ifndef TXN_REQ_FACTORY_H_
#define TXN_REQ_FACTORY_H_

namespace rococo {

class TxnRequest;

class TxnRequestFactory {
 private:
  typedef struct {
    int n_branch_;
    int n_teller_;
    int n_customer_;
  } tpca_para_t;

  typedef struct {
    int n_w_id_;
    int n_d_id_;
    int n_c_id_;
    int n_i_id_;
    int const_home_w_id_;
    int delivery_d_id_;
  } tpcc_para_t;

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
    tpcc_para_t tpcc_para_;
    rw_benchmark_para_t rw_benchmark_para_;
    micro_bench_para_t micro_bench_para_;
  };

  int benchmark_;
  int n_try_;
  int single_server_;
  int fix_id_;
  std::vector<double> &txn_weight_;
  Sharding* sharding_;

 public:
  TxnRequestFactory(Sharding* sd);

  void get_txn_req(TxnRequest *req, uint32_t cid) const;

  // rw_benchmark
  void get_rw_benchmark_txn_req(TxnRequest *req, uint32_t cid) const;
  void get_rw_benchmark_w_txn_req(TxnRequest *req, uint32_t cid) const;
  void get_rw_benchmark_r_txn_req(TxnRequest *req, uint32_t cid) const;

  // tpca
  void get_tpca_txn_req(TxnRequest *req, uint32_t cid) const;

  // tpcc
  void get_tpcc_txn_req(TxnRequest *req, uint32_t cid) const;
  // tpcc new_order
  void get_tpcc_new_order_txn_req(TxnRequest *req, uint32_t cid) const;
  // tpcc payment
  void get_tpcc_payment_txn_req(TxnRequest *req, uint32_t cid) const;
  // tpcc stock_level
  void get_tpcc_stock_level_txn_req(TxnRequest *req, uint32_t cid) const;
  // tpcc delivery
  void get_tpcc_delivery_txn_req(TxnRequest *req, uint32_t cid) const;
  // tpcc order_status
  void get_tpcc_order_status_txn_req(TxnRequest *req, uint32_t cid) const;

  void get_micro_bench_read_req(TxnRequest *req, uint32_t cid) const;
  void get_micro_bench_write_req(TxnRequest *req, uint32_t cid) const;
  void get_micro_bench_txn_req(TxnRequest *req, uint32_t cid) const;
  void get_txn_types(std::map<int32_t, std::string> &txn_types);

//  static TxnRequestFactory *txn_req_factory_s;

//  static void init_txn_req(TxnRequest *req, uint32_t cid);
//  static void destroy();

  ~TxnRequestFactory();
};

} // namespace rcc

#endif
