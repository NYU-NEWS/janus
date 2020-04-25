#include "procedure.h"

namespace janus {
    void DynamicChopper::Init(TxRequest &req) {
        verify(req.tx_type_ == DYNAMIC_ROTXN || req.tx_type_ == DYNAMIC_RW);
        type_ = req.tx_type_;
        ws_init_ = req.input_;
        ws_ = ws_init_;
        callback_ = req.callback_;
        max_try_ = req.n_try_;
        n_try_ = 1;
        commit_.store(true);
        dynamic_rw_reads = req.dynamic_rw_reads;
        switch (type_) {
            case DYNAMIC_ROTXN:
                DynamicRotxnInit(req);
                break;
            case DYNAMIC_RW:
                DynamicRWInit(req);
                break;
            default: verify(0);
                break;
        }
        verify(n_pieces_dispatchable_ > 0);
    }

    void DynamicChopper::DynamicRotxnInit(TxRequest &req) {
        int ol_cnt = ws_[DYNAMIC_TXN_SIZE].get_i32();
        n_pieces_all_ = ol_cnt;
        n_pieces_dispatchable_ =  ol_cnt;
        n_pieces_dispatch_acked_ = 0;
        n_pieces_dispatched_ = 0;
        for (int i = 0; i < ol_cnt; ++i) {
            status_[DYNAMIC_ROTXN_P(i)] = DISPATCHABLE;
            GetWorkspace(DYNAMIC_ROTXN_P(i)).keys_ = {DYNAMIC_ROTXN_KEY(i)};
            output_size_= {{DYNAMIC_ROTXN_P(i), 1}};
            p_types_ = {{DYNAMIC_ROTXN_P(i), DYNAMIC_ROTXN_P(i)}};
            sss_->GetPartition(DYNAMIC_TABLE, req.input_[DYNAMIC_ROTXN_KEY(i)],
                               sharding_[DYNAMIC_ROTXN_P(i)]);
        }
    }

    void DynamicChopper::DynamicRWInit(TxRequest &req) {
        int ol_cnt = ws_[DYNAMIC_TXN_SIZE].get_i32();
        n_pieces_all_ = ol_cnt;
        n_pieces_dispatchable_ =  ol_cnt;
        n_pieces_dispatch_acked_ = 0;
        n_pieces_dispatched_ = 0;
        for (int i = 0; i < ol_cnt; ++i) {
            status_[DYNAMIC_RW_P(i)] = DISPATCHABLE;
            GetWorkspace(DYNAMIC_RW_P(i)).keys_ = {
                    DYNAMIC_RW_KEY(i),
                    DYNAMIC_TXN_SIZE,
                    DYNAMIC_RW_W_COUNT
            };
            output_size_= {{DYNAMIC_RW_P(i), 0}};
            p_types_ = {{DYNAMIC_RW_P(i), DYNAMIC_RW_P(i)}};
            sss_->GetPartition(DYNAMIC_TABLE, req.input_[DYNAMIC_RW_KEY(i)],
                               sharding_[DYNAMIC_RW_P(i)]);
        }
    }

    void DynamicChopper::DynamicRotxnRetry() {
        int ol_cnt = ws_[DYNAMIC_TXN_SIZE].get_i32();
        n_pieces_all_ = ol_cnt;
        n_pieces_dispatchable_ =  ol_cnt;
        for (int i = 0; i < ol_cnt; ++i) {
            status_[DYNAMIC_ROTXN_P(i)] = DISPATCHABLE;
            GetWorkspace(DYNAMIC_ROTXN_P(i)).keys_ = {DYNAMIC_ROTXN_KEY(i)};
        }
    }

    void DynamicChopper::DynamicRWRetry() {
        int ol_cnt = ws_[DYNAMIC_TXN_SIZE].get_i32();
        n_pieces_all_ = ol_cnt;
        n_pieces_dispatchable_ =  ol_cnt;
        for (int i = 0; i < ol_cnt; ++i) {
            status_[DYNAMIC_RW_P(i)] = DISPATCHABLE;
            GetWorkspace(DYNAMIC_RW_P(i)).keys_ = {
                    DYNAMIC_RW_KEY(i),
                    DYNAMIC_TXN_SIZE,
                    DYNAMIC_RW_W_COUNT
            };
        }
    }

    void DynamicChopper::Reset() {
        TxData::Reset();
        ws_ = ws_init_;
        partition_ids_.clear();
        n_try_++;
        commit_.store(true);
        n_pieces_dispatchable_ = 0;
        n_pieces_dispatch_acked_ = 0;
        n_pieces_dispatched_ = 0;
        switch (type_) {
            case DYNAMIC_ROTXN:
                DynamicRotxnRetry();
                break;
            case DYNAMIC_RW:
                DynamicRWRetry();
                break;
            default: verify(0);
                break;
        }
    }

    bool DynamicChopper::IsReadOnly() {
        switch (type_) {
            case DYNAMIC_ROTXN:
                return true;
            case DYNAMIC_RW:
                return false;
            default: verify(0);
                return false;
        }
    }
} // namespace janus
