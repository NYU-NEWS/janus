#include "procedure.h"

namespace janus {
    void FBChopper::Init(TxRequest &req) {
        verify(req.tx_type_ == FB_ROTXN || req.tx_type_ == FB_WRITE);
        type_ = req.tx_type_;
        ws_init_ = req.input_;
        ws_ = ws_init_;
        callback_ = req.callback_;
        max_try_ = req.n_try_;
        n_try_ = 1;
        commit_.store(true);
        switch (type_) {
            case FB_ROTXN:
                FBRotxnInit(req);
                break;
            case FB_WRITE:
                FBWriteInit(req);
                break;
            default: verify(0);
                break;
        }
        verify(n_pieces_dispatchable_ > 0);
    }

    void FBChopper::FBRotxnInit(TxRequest &req) {
        int ol_cnt = ws_[FB_OP_COUNT].get_i32();
        n_pieces_all_ = ol_cnt;
        n_pieces_dispatchable_ =  ol_cnt;
        n_pieces_dispatch_acked_ = 0;
        n_pieces_dispatched_ = 0;
        for (int i = 0; i < ol_cnt; ++i) {
            status_[FB_ROTXN_P(i)] = DISPATCHABLE;
            GetWorkspace(FB_ROTXN_P(i)).keys_ = {FB_REQ_VAR_ID(i)};
            output_size_= {{FB_ROTXN_P(i), 1}};
            p_types_ = {{FB_ROTXN_P(i), FB_ROTXN_P(i)}};
            sss_->GetPartition(FB_TABLE, req.input_[FB_REQ_VAR_ID(i)],
                               sharding_[FB_ROTXN_P(i)]);
        }
    }

    void FBChopper::FBWriteInit(TxRequest &req) {
        int ol_cnt = ws_[FB_OP_COUNT].get_i32();
        verify(ol_cnt == 1);
        n_pieces_all_ = ol_cnt;
        n_pieces_dispatchable_ =  ol_cnt;
        n_pieces_dispatch_acked_ = 0;
        n_pieces_dispatched_ = 0;
        for (int i = 0; i < ol_cnt; ++i) { // 1 iteration
            status_[FB_WRITE_P(i)] = DISPATCHABLE;
            GetWorkspace(FB_WRITE_P(i)).keys_ = {FB_REQ_VAR_ID(i)};
            output_size_= {{FB_WRITE_P(i), 0}};
            p_types_ = {{FB_WRITE_P(i), FB_WRITE_P(i)}};
            sss_->GetPartition(FB_TABLE, req.input_[FB_REQ_VAR_ID(i)],
                               sharding_[FB_WRITE_P(i)]);
        }
    }

    void FBChopper::FBRotxnRetry() {
        int ol_cnt = ws_[FB_OP_COUNT].get_i32();
        n_pieces_all_ = ol_cnt;
        n_pieces_dispatchable_ =  ol_cnt;
        for (int i = 0; i < ol_cnt; ++i) {
            status_[FB_ROTXN_P(i)] = DISPATCHABLE;
            GetWorkspace(FB_ROTXN_P(i)).keys_ = {FB_REQ_VAR_ID(i)};
        }
    }

    void FBChopper::FBWriteRetry() {
        int ol_cnt = ws_[FB_OP_COUNT].get_i32();
        verify(ol_cnt == 1);
        n_pieces_all_ = ol_cnt;
        n_pieces_dispatchable_ =  ol_cnt;
        for (int i = 0; i < ol_cnt; ++i) { // 1 iteration
            status_[FB_WRITE_P(i)] = DISPATCHABLE;
            GetWorkspace(FB_WRITE_P(i)).keys_ = {FB_REQ_VAR_ID(i)};
        }
    }

    void FBChopper::Reset() {
        TxData::Reset();
        ws_ = ws_init_;
        partition_ids_.clear();
        n_try_++;
        commit_.store(true);
        /*
        for (auto& pair : status_) {
            pair.second = DISPATCHABLE;
        }
        */
        n_pieces_dispatchable_ = 0;
        n_pieces_dispatch_acked_ = 0;
        n_pieces_dispatched_ = 0;
        switch (type_) {
            case FB_ROTXN:
                FBRotxnRetry();
                break;
            case FB_WRITE:
                FBWriteRetry();
                break;
            default: verify(0);
                break;
        }
    }

    bool FBChopper::IsReadOnly() {
        switch (type_) {
            case FB_ROTXN:
                return true;
            case FB_WRITE:
                return false;
            default: verify(0);
                return false;
        }
    }
} // namespace janus
