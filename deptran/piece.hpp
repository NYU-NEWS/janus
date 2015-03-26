#pragma once

#include <string>

namespace rococo {

class Piece {
public:
    static Piece *get_piece(int benchmark);

    virtual void reg_all() = 0;

    virtual ~Piece() {}
};


#define BEGIN_PIE(txn, pie, iod) \
    TxnRegistry::reg(txn, pie, iod, \
        [] (DTxn *dtxn, \
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
            ((TPLDTxn*)dtxn)->get_2pl_succ_callback(header, input, input_size, res, ps); \
        std::function<void(void)> fail_callback = \
            ((TPLDTxn*)dtxn)->get_2pl_fail_callback(header, res, ps); \
        ps->reg_rw_lock( \
            std::vector<mdb::column_lock_t>({__VA_ARGS__}), \
            succ_callback, fail_callback); \
        return; \
    }

#define CREATE_ROW(schema, row_data) \
    switch (DTxnMgr::get_sole_mgr()->get_mode()) { \
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


} // namespace rcc
