#pragma once

#include "__dep__.h"
#include "tpl/exec.h"
#include "tpl/tpl.h"
#include "ro6/ro6.h"
#include "tpl/ps.h"

namespace rococo {

class TxnRegistry;

class Piece {
 public:
  Sharding* sss_ = nullptr;
  TxnRegistry *txn_reg_ = nullptr;
  static Piece *get_piece(int benchmark);
  virtual void reg_all() = 0;
  virtual ~Piece() { }
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

#define BEGIN_PIE(txn, pie, iod) \
  txn_reg_->reg(txn, pie, iod, \
        [this] (Executor* exec, \
                DTxn *dtxn, \
                SimpleCommand &cmd, \
                i32 *res, \
                map<int32_t, Value> &output)

#define END_PIE );

#define BEGIN_CB(txn_type, inn_id) \
txn_reg_->callbacks_[std::make_pair(txn_type, inn_id)] = \
[] (TxnCommand *ch, std::map<int32_t, Value> output) -> bool {

#define END_CB  };

#define SHARD_PIE(txn, pie, tb, ...) \
txn_reg_->sharding_input_[std::make_pair(txn, pie)] \
= std::make_pair(tb, vector<int32_t>({__VA_ARGS__}));

#define INPUT_PIE(txn, pie, ...) \
txn_reg_->input_vars_[txn][pie] = {__VA_ARGS__};

#define OUTPUT_PIE(txn, pie, ...) \
txn_reg_->output_vars_[txn][pie] = {__VA_ARGS__};

//std::vector<mdb::column_lock_t>(__VA_ARGS__),
//verify(((TPLDTxn*)dtxn)->locking_ == (output_size == nullptr));
//#define TPL_KISS(...) \
//  if (IS_MODE_2PL && ((TPLDTxn*)dtxn)->locking_) { \
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
//  if (IS_MODE_2PL && ((TPLDTxn*)dtxn)->locking_) { \
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
//  if (IS_MODE_2PL && ((TPLDTxn*)dtxn)->locking_) { \
//    PieceStatus *ps = ((TPLExecutor*)exec)->get_piece_status(header.pid); \
//    ((TPLExecutor*)exec)->get_2pl_succ_callback(header, input, res, ps)(); \
//    return; \
//  }

#define TPL_KISS(...) (0)
#define TPL_KISS_NONE (0)
#define TPL_KISS_ROW  (0)
//#define TPL_KISS_ROW(r) \
//  if (IS_MODE_2PL && ((TPLDTxn*)dtxn)->locking_) { \
//    ((TPLDTxn*)dtxn)->row_lock_ = r; \
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
    switch (Config::config_s->cc_mode_) { \
    case MODE_2PL: \
        r = mdb::FineLockedRow::create(schema, row_data); \
        break; \
        case MODE_OCC: \
    case MODE_NONE: \
        r = mdb::VersionedRow::create(schema, row_data); \
        break; \
    case MODE_RCC: \
        r = dtxn->CreateRow(schema, row_data); \
        break; \
    case MODE_RO6: \
        r = dtxn->CreateRow(schema, row_data); \
        break; \
    default: \
        r = dtxn->CreateRow(schema, row_data); \
        break; \
    }


#define IN_PHASE_1 (dtxn->phase_ == 1)
#define TPL_PHASE_1 (output_size == nullptr)
#define RO6_RO_PHASE_1 ((Config::GetConfig()->get_mode() == MODE_RO6) && ((RO6DTxn*)dtxn)->read_only_ && dtxn->phase_ == 1)


#define C_LAST_SCHEMA (((TpccSharding*)(this->sss_))->g_c_last_schema)

#define C_LAST2ID (((TpccSharding*)(this->sss_))->g_c_last2id)

} // namespace rcc
