#pragma once

#include "dtxn.h"

namespace rococo {

class TPLDTxn: public DTxn {
 public:

  TPLDTxn(i64 tid, DTxnMgr *mgr);

  int start_launch(
      const RequestHeader &header,
      const std::vector <mdb::Value> &input,
      const rrr::i32 &output_size,
      rrr::i32 *res,
      std::vector <mdb::Value> *output,
      rrr::DeferredReply *defer
  );

  int prepare_launch(
      const std::vector <i32> &sids,
      rrr::i32 *res,
      rrr::DeferredReply *defer
  );

  int prepare();

  int commit_launch(
      rrr::i32 *res,
      rrr::DeferredReply *defer
  );

  int commit();

  int abort_launch(
      rrr::i32 *res,
      rrr::DeferredReply *defer
  );

  int abort();

  // This method should not be used for now.
  virtual mdb::Row *create(const mdb::Schema *schema, const std::vector <mdb::Value> &values) {
    verify(0);
    return nullptr;
  }

  virtual bool read_column(
      mdb::Row *row,
      mdb::column_id_t col_id,
      Value *value
  );


  std::function<void(void)> get_2pl_proceed_callback(
      const RequestHeader &header,
      const mdb::Value *input,
      rrr::i32 input_size,
      rrr::i32 *res
  );

  std::function<void(void)> get_2pl_fail_callback(
      const RequestHeader &header,
      rrr::i32 *res,
      mdb::Txn2PL::PieceStatus *ps
  );

//    std::function<void(void)> get_2pl_succ_callback(
//            const RequestHeader &header,
//            const mdb::Value *input,
//            rrr::i32 input_size,
//            rrr::i32 *res,
//            mdb::Txn2PL::PieceStatus *ps,
//            std::function<void(
//                    const RequestHeader &,
//                    const Value *,
//                    rrr::i32,
//                    rrr::i32 *)> func
//    );

  std::function<void(void)> get_2pl_succ_callback(
      const RequestHeader &req,
      const mdb::Value *input,
      rrr::i32 input_size,
      rrr::i32 *res,
      mdb::Txn2PL::PieceStatus *ps
  );

  // Below are merged from TxnRegistry.
  void pre_execute_2pl(
      const RequestHeader &header,
      const std::vector <mdb::Value> &input,
      rrr::i32 *res,
      std::vector <mdb::Value> *output,
      DragonBall *db
  );


  void pre_execute_2pl(
      const RequestHeader &header,
      const Value *input,
      rrr::i32 input_size,
      rrr::i32 *res,
      mdb::Value *output,
      rrr::i32 *output_size,
      DragonBall *db
  );

  void execute(
      const RequestHeader &header,
      const std::vector <mdb::Value> &input,
      rrr::i32 *res,
      std::vector <mdb::Value> *output
  ) {
    rrr::i32 output_size = output->size();
    txn_reg_->get(header).txn_handler(
        this, header, input.data(), input.size(),
        res, output->data(), &output_size, NULL);
    output->resize(output_size);
  }

  inline void execute(
      const RequestHeader &header,
      const Value *input,
      rrr::i32 input_size,
      rrr::i32 *res,
      mdb::Value *output,
      rrr::i32 *output_size
  ) {
    txn_reg_->get(header).txn_handler(
        this, header, input, input_size,
        res, output, output_size, NULL);
  }
};

} // namespace rococo
