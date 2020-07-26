
#include "bench/tpca/workload.h"
#include "bench/tpca/payment.h"

namespace janus {
void TpcaPaymentChopper::Init(TxRequest &req) {
  verify(req.tx_type_ == TPCA_PAYMENT);
  type_ = TPCA_PAYMENT;
  ws_init_ = req.input_;
  ws_ = ws_init_;

  Value& cus = req.input_[TPCA_VAR_X];
  Value& tel = req.input_[TPCA_VAR_Y];
  Value& bra = req.input_[TPCA_VAR_Z];

  GetWorkspace(TPCA_PAYMENT_1).keys_ = {TPCA_VAR_X};
  GetWorkspace(TPCA_PAYMENT_2).keys_ = {TPCA_VAR_Y};
  GetWorkspace(TPCA_PAYMENT_3).keys_ = {TPCA_VAR_Z};

  output_size_ = {
      {TPCA_PAYMENT_1, 0},
      {TPCA_PAYMENT_2, 0},
      {TPCA_PAYMENT_3, 0}
  };

  this->p_types_ = {
      {TPCA_PAYMENT_1, TPCA_PAYMENT_1},
      {TPCA_PAYMENT_2, TPCA_PAYMENT_2},
      {TPCA_PAYMENT_3, TPCA_PAYMENT_3}
  };

  sss_->GetPartition(TPCA_CUSTOMER, cus, sharding_[TPCA_PAYMENT_1]);
  sss_->GetPartition(TPCA_CUSTOMER, tel, sharding_[TPCA_PAYMENT_2]);
  sss_->GetPartition(TPCA_CUSTOMER, bra, sharding_[TPCA_PAYMENT_3]);

  // all pieces are ready
  n_pieces_all_ = 3;
  callback_ = req.callback_;
  max_try_ = req.n_try_;
  n_try_ = 1;
  n_pieces_dispatchable_ = 3;

  status_ = {
      {TPCA_PAYMENT_1, DISPATCHABLE},
      {TPCA_PAYMENT_2, DISPATCHABLE},
      {TPCA_PAYMENT_3, DISPATCHABLE}
  };
  ranks_ = {
      {TPCA_PAYMENT_1, RANK_I},
      {TPCA_PAYMENT_2, RANK_D},
      {TPCA_PAYMENT_3, RANK_D}
  };

  commit_.store(true);
}

void TpcaPaymentChopper::Reset() {
  TxData::Reset();
  n_pieces_dispatchable_ = 3;
  status_ = {
      {TPCA_PAYMENT_1, DISPATCHABLE},
      {TPCA_PAYMENT_2, DISPATCHABLE},
      {TPCA_PAYMENT_3, DISPATCHABLE}
  };
  commit_.store(true);
  partition_ids_.clear();
  n_try_++;
}


} // namespace janus
