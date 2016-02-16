#pragma once

namespace rococo {

class TpccChopper;

class TpccRealDistChopper: public TpccChopper {
 protected:
  virtual parid_t GetPiecePar(innid_t inn_id);

 public:
  TpccRealDistChopper();
  virtual ~TpccRealDistChopper();
};

} // namespace rococo
