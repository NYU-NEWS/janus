
#include "./bench/tpca/piece.h"
#include "./bench/tpca/chopper.h"

namespace rococo {
void TpcaPaymentChopper::Init(TxnRequest &req) {
  verify(req.txn_type_ == TPCA_PAYMENT);
  type_ = TPCA_PAYMENT;

  Value& cus = req.input_[0];
  Value& tel = req.input_[1];
  Value& bra = req.input_[2];

  inputs_.clear();
  inputs_[TPCA_PAYMENT_1] = {{TPCA_VAR_X, cus}/*, inc*/};
  inputs_[TPCA_PAYMENT_2] = {{TPCA_VAR_Y, tel}/*, inc*/};
  inputs_[TPCA_PAYMENT_3] = {{TPCA_VAR_Z, bra}/*, inc*/};

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
  n_pieces_input_ready_ = 3;

  status_ = {
      {TPCA_PAYMENT_1, READY},
      {TPCA_PAYMENT_2, READY},
      {TPCA_PAYMENT_3, READY}
  };
  commit_.store(true);
}

void TpcaPaymentChopper::Reset() {
  TxnCommand::Reset();
  n_pieces_input_ready_ = 3;
  status_ = {
      {TPCA_PAYMENT_1, READY},
      {TPCA_PAYMENT_2, READY},
      {TPCA_PAYMENT_3, READY}
  };
  commit_.store(true);
  partition_ids_.clear();
  n_try_++;
}


} // namespace rococo
