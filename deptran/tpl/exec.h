#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../classic/exec.h"

namespace rococo {

class PieceStatus;
class TplExecutor: public ClassicExecutor {
  using ClassicExecutor::ClassicExecutor;
 public:
  bool prepared_ = false;
  bool wounded_ = false;
  std::unordered_map<i64, PieceStatus *> piece_map_ = {};
  PieceStatus *ps_cache_ = nullptr;

  PieceStatus* ps_cache();
  void SetPsCache(PieceStatus*);
  PieceStatus *get_piece_status(i64 pid);
  void release_piece_map(bool commit);
  void InitPieceStatus(const SimpleCommand &cmd,
                       const function<void()>& callback,
                       std::map<int32_t, Value> *output);

  virtual ~TplExecutor(){};

  int OnDispatch(const SimpleCommand &cmd,
                 rrr::i32 *res,
                 map<int32_t, Value> *output,
                 const function<void()> &callback) override;

  // Below are merged from TxnRegistry.

  void LockSucceeded(const SimpleCommand& cmd,
                     rrr::i32 *res,
                     PieceStatus *ps);

  void LockFailed(const SimpleCommand& cmd,
                  rrr::i32 *res,
                  PieceStatus *ps);

  virtual bool Prepare() override;
  virtual int Commit() override;
  virtual int Abort() override;

};

} // namespace rococo