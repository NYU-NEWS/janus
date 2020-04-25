#pragma once

#include "__dep__.h"
#include "constants.h"
#include "config.h"
#include "txn_reg.h"
#include "2pl/tx.h"
#include "snow/ro6.h"
#include "access/row.h"

namespace janus {

class TxRequest;
class Sharding;

class Workload {
 public:
  typedef struct {
      int n_friends_;
  } fb_para_t;

  typedef struct {
      int n_directories_;
  } spanner_para_t;

  typedef struct {
      int n_rows_;
  } dynamic_para_t;

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
    fb_para_t fb_para_;
    spanner_para_t spanner_para_;
    dynamic_para_t dynamic_para_;
    tpca_para_t tpca_para_;
    rw_benchmark_para_t rw_benchmark_para_;
    micro_bench_para_t micro_bench_para_;
  };

  int benchmark_;
  int n_try_;
  int single_server_;
  int fix_id_ = -1;
  std::vector<double>& txn_weight_;
  std::map<string, double>& txn_weights_;
  Sharding* sharding_;
  Sharding* sss_ = nullptr;
  shared_ptr<TxnRegistry> txn_reg_{nullptr};

 public:
  static Workload *CreateWorkload(Config *config);

 public:
  Workload() = delete;
  Workload(Config* config);
  virtual ~Workload();

  virtual void GetTxRequest(TxRequest* req, uint32_t cid) = 0;
  virtual void GetProcedureTypes(std::map<int32_t, std::string> &txn_types);
  virtual void RegisterPrecedures() = 0;

  /*
   * inn_id is piece_type for now. better change in the future.
   */
  void RegP(txntype_t txn_type,
            innid_t inn_id,
            const set<int32_t>& ivars,
            const set<int32_t>& ovars,
            const vector<conf_id_t>& conflicts,
            const sharder_t& sharder,
            const rank_t& rank,
            const ProcHandler& handler
  ) {
    auto& piece = txn_reg_->regs_[txn_type][inn_id];
    piece.input_vars_ = ivars;
    piece.output_vars_ = ovars;
    piece.conflicts_ = conflicts;
    piece.sharder_ = sharder;
    piece.rank_ = rank;
    piece.proc_handler_ = handler;
  }

  /*
   * For acc ML engine recording each piece is a read or a write
   */
  void set_op_type(txntype_t txn_type, innid_t inn_id, optype_t op_type = UNDEFINED) const {
      auto& piece = txn_reg_->regs_[txn_type][inn_id];
      piece.op_type = op_type;
  }

  void set_write_only(txntype_t txn_type, innid_t inn_id) const {
      auto& piece = txn_reg_->regs_[txn_type][inn_id];
      piece.write_only_ = 1;  // set true
  }
};

#define BEGIN_LOOP_PIE(txn, pie, max_i, iod) \
for (int I = 0; I < max_i; I++) { \
txn_reg_->reg(txn, pie + I, iod, \
[this, I] ( \
Executor* exec, \
DTxn *dtxn, \
SimpleCommand& cmd, \
i32 *res, \
map<int32_t, Value> &output) \
{

#define END_LOOP_PIE });}

#define PROC \
  [this] (Executor* exec, Tx& tx, SimpleCommand& cmd, \
          int32_t *res, map<int32_t, Value> &output)

#define LPROC \
  [this, i] (Executor* exec, Tx& tx, SimpleCommand& cmd, \
          int32_t *res, map<int32_t, Value> &output)

#define BEGIN_CB(txn_type, inn_id) \
txn_reg_->regs_[txn_type][inn_id].callback_ = \
[] (TxData *ch, std::map<int32_t, Value> output) -> bool {

#define END_CB  };

#define INPUT_PIE(txn, pie, ...) \
txn_reg_->regs_[txn][pie].input_vars_ \
= {__VA_ARGS__};

#define OUTPUT_PIE(txn, pie, ...) \
txn_reg_->regs_[txn][pie].output_vars_ \
= {__VA_ARGS__};

#define CONFLICT_PIE(txn, pie, ...) \
txn_reg_->regs_[txn][pie].conflicts_ \
= {__VA_ARGS__};

#define SHARD_PIE(txn, pie, tb, ...) \
txn_reg_->regs_[txn][pie].sharder_ \
= std::make_pair(tb, vector<int32_t>({__VA_ARGS__}));


//std::vector<mdb::column_lock_t>(__VA_ARGS__),
//verify(((TplTxBox*)dtxn)->locking_ == (output_size == nullptr));
//#define TPL_KISS(...) \
//  if (IS_MODE_2PL && ((TplTxBox*)dtxn)->locking_) { \
//    PieceStatus *ps \
//        = ((TPLExecutor*)exec)->get_piece_status(header.pid); \
//    std::function<void(void)> succ_callback = \
//        ((TPLExecutor*)exec)->get_2pl_succ_callback(header, input, res, ps); \
//    std::function<void(void)> fail_callback = \
//        ((TPLExecutor*)exec)->get_2pl_fail_callback(header, res, ps); \
//    ps->reg_rw_lock( \
//    std::vector<mdb::column_lock_t>({__VA_ARGS__}),\
//        succ_callback, fail_callback); \
//    return; \
//  }
//#define TPL_KISS_ROW(r) \
//  if (IS_MODE_2PL && ((TplTxBox*)dtxn)->locking_) { \
//    PieceStatus *ps = ((TPLExecutor*)exec)->get_piece_status(header.pid); \
//    std::function<void(void)> succ_callback = \
//      ((TPLExecutor*) exec)->get_2pl_succ_callback( \
//        header, input, res, ps); \
//    std::function<void(void)> fail_callback = \
//      ((TPLExecutor*) exec)->get_2pl_fail_callback( \
//        header, res, ps); \
//    ps->reg_rm_lock(r, succ_callback, fail_callback); \
//    return; \
//}

//#define TPL_KISS_NONE \
//  if (IS_MODE_2PL && ((TplTxBox*)dtxn)->locking_) { \
//    PieceStatus *ps = ((TPLExecutor*)exec)->get_piece_status(header.pid); \
//    ((TPLExecutor*)exec)->get_2pl_succ_callback(header, input, res, ps)(); \
//    return; \
//  }

#define TPL_KISS(...) (0)
#define TPL_KISS_NONE (0)
#define TPL_KISS_ROW  (0)
//#define TPL_KISS_ROW(r) \
//  if (IS_MODE_2PL && ((TplTxBox*)dtxn)->locking_) { \
//    ((TplTxBox*)dtxn)->row_lock_ = r; \
//    return; \
//}


#define RCC_KISS(row, col, imdt) \
    if ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1) { \
        verify(row != nullptr); \
        ((RCCDTxn*)dtxn)->kiss(row, col, imdt); \
    }

#define RCC_PHASE1_RET \
    { \
        if ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1) { \
            Log::debug("RETURN mode is RCC or RO6 and in phase 1\n"); \
            return; \
        } \
    } while(0);

#define RCC_SAVE_ROW(row, index) \
  if ((IS_MODE_RCC || IS_MODE_RO6) && IN_PHASE_1) { \
    auto &row_map = ((RCCDTxn*)dtxn)->dreqs_.back().row_map; \
    auto ret = row_map.insert(std::pair<int, mdb::Row*>(index, row)); \
    verify(ret.second); \
    verify(row->schema_); \
  }

#define RCC_LOAD_ROW(row, index) \
  if ((IS_MODE_RCC || IS_MODE_RO6) && !(IN_PHASE_1)) { \
    auto &row_map = ((RCCDTxn*)dtxn)->dreqs_.back().row_map; \
    auto it = row_map.find(index); \
    verify(it != row_map.end()); \
    row = it->second; \
    verify(row->schema_); \
  }

#define CREATE_ROW(schema, row_data) \
    switch (Config::config_s->tx_proto_) { \
    case MODE_2PL: \
        r = mdb::FineLockedRow::create(schema, row_data); \
        break; \
        case MODE_OCC: \
    case MODE_NONE: \
        r = mdb::VersionedRow::create(schema, row_data); \
        break; \
    case MODE_RCC: \
        r = tx.CreateRow(schema, row_data); \
        break; \
    case MODE_RO6: \
        r = tx.CreateRow(schema, row_data); \
        break; \
    case MODE_ACC: \
        r = AccRow::create(schema, row_data); \
        break; \
    default: \
        r = tx.CreateRow(schema, row_data); \
        break; \
    }


#define IN_PHASE_1 (dtxn->phase_ == 1)
#define TPL_PHASE_1 (output_size == nullptr)
#define RO6_RO_PHASE_1 ((Config::GetConfig()->get_mode() == MODE_RO6) && ((RO6DTxn*)dtxn)->read_only_ && dtxn->phase_ == 1)


} // namespace janus
