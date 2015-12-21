
#include "./bench/tpca/piece.h"
#include "./bench/tpca/chopper.h"

namespace rococo {

void TpcaPaymentChopper::init(TxnRequest &req) {
  verify(req.txn_type_ == TPCA_PAYMENT);
  txn_type_ = TPCA_PAYMENT;

  Value& cus = req.input_[0];
  Value& tel = req.input_[1];
  Value& bra = req.input_[2];
  //Value& inc = req.input_[3];

  inputs_.clear();
  inputs_[0] = {{0, cus}/*, inc*/};
  inputs_[1] = {{0, tel}/*, inc*/};
  inputs_[2] = {{0, bra}/*, inc*/};

  output_size_.assign({0, 0, 0});

  this->p_types_ = {TPCA_PAYMENT_1, TPCA_PAYMENT_2, TPCA_PAYMENT_3};

  // get sharding info
  sharding_ = {0, 0, 0};
  sss_->get_site_id_from_tb(TPCA_CUSTOMER, cus, sharding_[0]);
  sss_->get_site_id_from_tb(TPCA_TELLER, tel, sharding_[1]);
  sss_->get_site_id_from_tb(TPCA_BRANCH, bra, sharding_[2]);

  // all pieces are ready
  n_pieces_all_ = 3;
  callback_ = req.callback_;
  max_try_ = req.n_try_;
  n_try_ = 1;


  status_ = std::vector<CommandStatus>(3, READY);
  commit_.store(true);

}

} // namespace rococo
