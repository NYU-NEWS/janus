#pragma once

#include "coordinator.h"

namespace rococo {

class TpcaPaymentChopper : public TxnChopper {

public:

    TpcaPaymentChopper() {
    }

    virtual void init(TxnRequest &req) {
        verify(req.txn_type_ == TPCA_PAYMENT);
        txn_type_ = TPCA_PAYMENT;

        Value& cus = req.input_[0];
        Value& tel = req.input_[1];
        Value& bra = req.input_[2];
        //Value& inc = req.input_[3];

        inputs_.clear();
        inputs_.push_back({cus/*, inc*/});
        inputs_.push_back({tel/*, inc*/});
        inputs_.push_back({bra/*, inc*/});

        output_size_.assign({0, 0, 0});

        this->p_types_ = {TPCA_PAYMENT_1, TPCA_PAYMENT_2, TPCA_PAYMENT_3};

        // get sharding info
        sharding_ = {0, 0, 0};
        Sharding::get_site_id(TPCA_CUSTOMER, cus, sharding_[0]);
        Sharding::get_site_id(TPCA_TELLER, tel, sharding_[1]);
        Sharding::get_site_id(TPCA_BRANCH, bra, sharding_[2]);

        // all pieces are ready
        n_pieces_ = 3;
        callback_ = req.callback_;
        max_try_ = req.n_try_;
        n_try_ = 1;


        status_ = {0, 0, 0};
        commit_.store(true);

    }

    virtual bool start_callback(const std::vector<int> &pi, int res, BatchStartArgsHelper &bsah) {
        return false;
    }

    virtual bool start_callback(int pi, int res, const std::vector<mdb::Value> &output) { return false; }

    virtual bool is_read_only() { return false; }

    virtual void retry() {
        n_started_ = 0;
        n_prepared_ = 0;
        n_finished_ = 0;
        status_ = {0, 0, 0};
        commit_.store(true);
        proxies_.clear();
        n_try_ ++;
    }

    virtual ~TpcaPaymentChopper() {}

};

} // namespace rococo
