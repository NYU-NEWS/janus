#pragma once

#include "__dep__.h"
#include "tpl/exec.h"
#include "tpl/tpl.h"
#include "ro6/ro6.h"

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


#define BEGIN_PIE(txn, pie, iod) \
  txn_reg_->reg(txn, pie, iod, \
        [this] (Executor* exec, \
DTxn *dtxn, \
                const RequestHeader &header, \
            const Value *input, \
            i32 input_size, \
            i32 *res, \
            Value *output, \
            i32 *output_size, \
            row_map_t *row_map)
#define END_PIE );

#define TPL_KISS(...) \
    if (IS_MODE_2PL && output_size == NULL) { \
        mdb::Txn2PL *tpl_txn = (mdb::Txn2PL *)dtxn->mdb_txn_; \
        mdb::Txn2PL::PieceStatus *ps \
            = tpl_txn->get_piece_status(header.pid); \
        std::function<void(void)> succ_callback = \
            ((TPLExecutor*)exec)->get_2pl_succ_callback(header, input, input_size, res, ps); \
        std::function<void(void)> fail_callback = \
            ((TPLExecutor*)exec)->get_2pl_fail_callback(header, res, ps); \
        ps->reg_rw_lock( \
            std::vector<mdb::column_lock_t>({__VA_ARGS__}), \
            succ_callback, fail_callback); \
        return; \
    }

#define TPL_KISS_NONE \
    if (IS_MODE_2PL && output_size == NULL) { \
        ((TPLExecutor*)exec)->get_2pl_proceed_callback( \
                header, input, input_size, res)(); \
        return; \
    }

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
        auto ret = row_map->insert(std::pair<int, mdb::Row*>(index, row)); \
        verify(ret.second); \
        verify(row->schema_); \
    }

#define RCC_LOAD_ROW(row, index) \
    if ((IS_MODE_RCC || IS_MODE_RO6) && !(IN_PHASE_1)) { \
        auto it = row_map->find(index); \
        verify(it != row_map->end()); \
        row = it->second; \
        verify(row->schema_); \
    }

#define CREATE_ROW(schema, row_data) \
    switch (Config::config_s->mode_) { \
    case MODE_2PL: \
        r = mdb::FineLockedRow::create(schema, row_data); \
        break; \
        case MODE_OCC: \
    case MODE_NONE: \
        r = mdb::VersionedRow::create(schema, row_data); \
        break; \
    case MODE_RCC: \
        r = dtxn->create(schema, row_data); \
        break; \
    case MODE_RO6: \
        r = dtxn->create(schema, row_data); \
        break; \
    default: \
        verify(0); \
    }


#define IN_PHASE_1 (dtxn->phase_ == 1)
#define TPL_PHASE_1 (output_size == nullptr)
#define RO6_RO_PHASE_1 ((Config::GetConfig()->get_mode() == MODE_RO6) && ((RO6DTxn*)dtxn)->read_only_ && dtxn->phase_ == 1)


#define C_LAST_SCHEMA (((TPCCDSharding*)(this->sss_))->g_c_last_schema)

#define C_LAST2ID (((TPCCDSharding*)(this->sss_))->g_c_last2id)

} // namespace rcc
