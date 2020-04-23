#include "procedure.h"

namespace janus {
    void SpannerChopper::Init(TxRequest &req) {
        verify(req.tx_type_ == SPANNER_ROTXN || req.tx_type_ == SPANNER_RW);
        type_ = req.tx_type_;
        ws_init_ = req.input_;
        ws_ = ws_init_;
        callback_ = req.callback_;
        max_try_ = req.n_try_;
        n_try_ = 1;
        commit_.store(true);
        spanner_rw_reads = req.spanner_rw_reads;
        switch (type_) {
            case SPANNER_ROTXN:
                SpannerRotxnInit(req);
                break;
            case SPANNER_RW:
                SpannerRWInit(req);
                break;
            default: verify(0);
                break;
        }
        verify(n_pieces_dispatchable_ > 0);
    }

    void SpannerChopper::SpannerRotxnInit(TxRequest &req) {
        int ol_cnt = ws_[SPANNER_TXN_SIZE].get_i32();
        n_pieces_all_ = ol_cnt;
        n_pieces_dispatchable_ =  ol_cnt;
        n_pieces_dispatch_acked_ = 0;
        n_pieces_dispatched_ = 0;
        for (int i = 0; i < ol_cnt; ++i) {
            status_[SPANNER_ROTXN_P(i)] = DISPATCHABLE;
            GetWorkspace(SPANNER_ROTXN_P(i)).keys_ = {SPANNER_ROTXN_KEY(i)};
            output_size_= {{SPANNER_ROTXN_P(i), 1}};
            p_types_ = {{SPANNER_ROTXN_P(i), SPANNER_ROTXN_P(i)}};
            sss_->GetPartition(SPANNER_TABLE, req.input_[SPANNER_ROTXN_KEY(i)],
                               sharding_[SPANNER_ROTXN_P(i)]);
        }
    }

    void SpannerChopper::SpannerRWInit(TxRequest &req) {
        int ol_cnt = ws_[SPANNER_TXN_SIZE].get_i32();
        n_pieces_all_ = ol_cnt;
        n_pieces_dispatchable_ =  ol_cnt;
        n_pieces_dispatch_acked_ = 0;
        n_pieces_dispatched_ = 0;
        for (int i = 0; i < ol_cnt; ++i) {
            status_[SPANNER_RW_P(i)] = DISPATCHABLE;
            GetWorkspace(SPANNER_RW_P(i)).keys_ = {
                    SPANNER_RW_KEY(i),
                    SPANNER_TXN_SIZE,
                    SPANNER_RW_W_COUNT
            };
            output_size_= {{SPANNER_RW_P(i), 0}};
            p_types_ = {{SPANNER_RW_P(i), SPANNER_RW_P(i)}};
            sss_->GetPartition(SPANNER_TABLE, req.input_[SPANNER_RW_KEY(i)],
                               sharding_[SPANNER_RW_P(i)]);
        }
    }

    void SpannerChopper::SpannerRotxnRetry() {
        int ol_cnt = ws_[SPANNER_TXN_SIZE].get_i32();
        n_pieces_all_ = ol_cnt;
        n_pieces_dispatchable_ =  ol_cnt;
        for (int i = 0; i < ol_cnt; ++i) {
            status_[SPANNER_ROTXN_P(i)] = DISPATCHABLE;
            GetWorkspace(SPANNER_ROTXN_P(i)).keys_ = {SPANNER_ROTXN_KEY(i)};
        }
    }

    void SpannerChopper::SpannerRWRetry() {
        int ol_cnt = ws_[SPANNER_TXN_SIZE].get_i32();
        n_pieces_all_ = ol_cnt;
        n_pieces_dispatchable_ =  ol_cnt;
        for (int i = 0; i < ol_cnt; ++i) {
            status_[SPANNER_RW_P(i)] = DISPATCHABLE;
            GetWorkspace(SPANNER_RW_P(i)).keys_ = {
                    SPANNER_RW_KEY(i),
                    SPANNER_TXN_SIZE,
                    SPANNER_RW_W_COUNT
            };
        }
    }

    void SpannerChopper::Reset() {
        TxData::Reset();
        ws_ = ws_init_;
        partition_ids_.clear();
        n_try_++;
        commit_.store(true);
        n_pieces_dispatchable_ = 0;
        n_pieces_dispatch_acked_ = 0;
        n_pieces_dispatched_ = 0;
        switch (type_) {
            case SPANNER_ROTXN:
                SpannerRotxnRetry();
                break;
            case SPANNER_RW:
                SpannerRWRetry();
                break;
            default: verify(0);
                break;
        }
    }

    bool SpannerChopper::IsReadOnly() {
        switch (type_) {
            case SPANNER_ROTXN:
                return true;
            case SPANNER_RW:
                return false;
            default: verify(0);
                return false;
        }
    }
} // namespace janus
