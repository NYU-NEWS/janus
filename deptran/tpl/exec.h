#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../three_phase/exec.h"

namespace rococo {

class PieceStatus;
class TPLExecutor: public ThreePhaseExecutor {
  using ThreePhaseExecutor::ThreePhaseExecutor;
 public:
  bool prepared_ = false;
  bool wounded_ = false;
  std::unordered_map<i64, PieceStatus *> piece_map_ = {};
  //  static PieceStatus *ps_cache_s; // TODO fix
  PieceStatus *ps_cache_ = nullptr;

  PieceStatus* ps_cache();
  void SetPsCache(PieceStatus*);
  PieceStatus *get_piece_status(i64 pid);
  void release_piece_map(bool commit);
  void init_piece(i64 tid,
                  i64 pid,
                  rrr::DragonBall *db,
                  std::map<int32_t, Value> *output);

  virtual ~TPLExecutor(){};

  virtual int start_launch(
      const RequestHeader &header,
      const map<int32_t, Value> &input,
      const rrr::i32 &output_size,
      rrr::i32 *res,
      map <int32_t, Value> *output,
      rrr::DeferredReply *defer
  );

  // Below are merged from TxnRegistry.

  std::function<void(void)> get_2pl_proceed_callback(
      const RequestHeader &header,
      const map<int32_t, Value> &input,
      rrr::i32 *res
  );

  std::function<void(void)> get_2pl_fail_callback(
      const RequestHeader &header,
      rrr::i32 *res,
      PieceStatus *ps
  );

  std::function<void(void)> get_2pl_succ_callback(
      const RequestHeader &req,
      const map<int32_t, Value> &input,
      rrr::i32 *res,
      PieceStatus *ps
  );

  virtual int prepare();
  virtual int commit();
  virtual int abort();

};

} // namespace rococo