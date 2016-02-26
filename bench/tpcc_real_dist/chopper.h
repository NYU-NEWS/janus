#pragma once

namespace rococo {

class TpccChopper;

class TpccRealDistChopper: public TpccChopper {
 protected:
  virtual siteid_t GetPiecePar(innid_t inn_id);

 public:
  TpccRealDistChopper();
  bool IsOneRound() override;
  virtual ~TpccRealDistChopper();
};

} // namespace rococo
