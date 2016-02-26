
#include "./bench/tpca/piece.h"
#include "./bench/tpca/chopper.h"

namespace rococo {
void TpcaPaymentChopper::init(TxnRequest &req) {
  verify(req.txn_type_ == TPCA_PAYMENT);
  type_ = TPCA_PAYMENT;

  Value& cus = req.input_[0];
  Value& tel = req.input_[1];
  Value& bra = req.input_[2];

  inputs_.clear();
  inputs_[0] = {{0, cus}/*, inc*/};
  inputs_[1] = {{0, tel}/*, inc*/};
  inputs_[2] = {{0, bra}/*, inc*/};

  output_size_ = {
      {0, 0},
      {1, 0},
      {2, 0}
  };

  this->p_types_ = {
      {0, TPCA_PAYMENT_1},
      {1, TPCA_PAYMENT_2},
      {2, TPCA_PAYMENT_3}
  };

  sharding_[TPCA_PAYMENT_1] = ChooseRandom(sss_->SiteIdsForKey(TPCA_CUSTOMER, cus));
  sharding_[TPCA_PAYMENT_2] = ChooseRandom(sss_->SiteIdsForKey(TPCA_TELLER, tel));
  sharding_[TPCA_PAYMENT_3] = ChooseRandom(sss_->SiteIdsForKey(TPCA_BRANCH, bra));

  // all pieces are ready
  n_pieces_all_ = 3;
  callback_ = req.callback_;
  max_try_ = req.n_try_;
  n_try_ = 1;

  status_ = {
      {TPCA_PAYMENT_1, READY},
      {TPCA_PAYMENT_2, READY},
      {TPCA_PAYMENT_3, READY}
  };
  commit_.store(true);
}
} // namespace rococo
