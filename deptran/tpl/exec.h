#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../classic/exec.h"

namespace rococo {

class PieceStatus;
class TPLExecutor: public ClassicExecutor {
  using ClassicExecutor::ClassicExecutor;
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
  void InitPieceStatus(const SimpleCommand &cmd,
                       const function<void()>& callback,
                       std::map<int32_t, Value> *output);

  virtual ~TPLExecutor(){};

  int StartLaunch(const SimpleCommand &cmd,
                  rrr::i32 *res,
                  map<int32_t, Value> *output,
                  const function<void()>& callback) override;

  // Below are merged from TxnRegistry.

  std::function<void(void)> get_2pl_fail_callback(
      const SimpleCommand& cmd,
      rrr::i32 *res,
      PieceStatus *ps
  );

  std::function<void(void)> get_2pl_succ_callback(
      const SimpleCommand& cmd,
      rrr::i32 *res,
      PieceStatus *ps
  );

  virtual bool Prepare() override;
  virtual int Commit() override;
  virtual int Abort() override;

};

} // namespace rococo