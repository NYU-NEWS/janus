#include "all.h"

namespace deptran {

void RWChopper::W_txn_init(TxnRequest &req) {
    inputs_.assign({
            std::vector<Value>({req.input_[0]/*, req.input_[1]*/})
            });
    output_size_.assign({
            0
            });
    p_types_ = {RW_BENCHMARK_W_TXN_0};
    sharding_.resize(1);
    sss_->get_site_id_from_tb(RW_BENCHMARK_TABLE, req.input_[0], sharding_[0]);
    status_ = {READY};
    n_pieces_all_ = 1;
}

void RWChopper::R_txn_init(TxnRequest &req) {
    inputs_.assign({
            std::vector<Value>({req.input_[0]})
            });
    output_size_.assign({
            1
            });
    p_types_ = {RW_BENCHMARK_R_TXN_0};
    sharding_.resize(1);
    sss_->get_site_id_from_tb(RW_BENCHMARK_TABLE, req.input_[0], sharding_[0]);
    status_ = {READY};
    n_pieces_all_ = 1;
}

RWChopper::RWChopper() {
}

void RWChopper::init(TxnRequest &req) {
    txn_type_ = req.txn_type_;
    callback_ = req.callback_;
    max_try_ = req.n_try_;
    n_try_ = 1;
    commit_.store(true);
    switch (req.txn_type_) {
        case RW_BENCHMARK_W_TXN:
            W_txn_init(req);
            break;
        case RW_BENCHMARK_R_TXN:
            R_txn_init(req);
            break;
        default:
            verify(0);
    }
}

bool RWChopper::start_callback(const std::vector<int> &pi, int res, BatchStartArgsHelper &bsah) {
    return false;
}

bool RWChopper::start_callback(int pi, int res, const std::vector<mdb::Value> &output) {
    return false;
}

bool RWChopper::is_read_only() {
    if (txn_type_ == RW_BENCHMARK_W_TXN)
        return false;
    else if (txn_type_ == RW_BENCHMARK_R_TXN)
        return true;
    else
        verify(0);
}

void RWChopper::retry() {
    status_ = {READY};
    commit_.store(true);
    partitions_.clear();
    n_pieces_out_ = 0;
    n_try_++;
}

RWChopper::~RWChopper() {
}

}
