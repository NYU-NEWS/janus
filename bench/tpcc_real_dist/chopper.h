#pragma once

namespace rococo {

class TpccTxn;

class TpccRealDistChopper: public TpccTxn {
 protected:
  virtual siteid_t GetPiecePar(innid_t inn_id);

 public:
  TpccRealDistChopper();
  bool IsOneRound() override;
  virtual ~TpccRealDistChopper();
};

} // namespace rococo
