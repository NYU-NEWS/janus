#pragma once

#include "../tpcc/chopper.h"

namespace rococo {

class TpccRealDistChopper: public TpccTxn {
 protected:
  virtual siteid_t GetPiecePar(innid_t inn_id);

 public:
  TpccRealDistChopper();
  bool IsOneRound() override;
  virtual ~TpccRealDistChopper();
};

} // namespace rococo
