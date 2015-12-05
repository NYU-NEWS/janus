#pragma once

#include "../__dep__.h"
#include "../executor.h"
#include "../rcc_rpc.h"
#include "../txn_reg.h"


namespace rococo {

class ThreePhaseExecutor: public Executor {
  using Executor::Executor;
 public:
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
        this, dtxn_, header, input.data(), input.size(),
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
        this, dtxn_, header, input, input_size,
        res, output, output_size, NULL);
  }

};

} // namespace rococo