#pragma once

#include "../tpcc/piece.h"

namespace rococo {

class TpccRealDistPiece: public TpccPiece {
 public:
  using TpccPiece::TpccPiece;
  void RegNewOrder() override;
  void RegPayment() override;
  virtual ~TpccRealDistPiece() { }
};

} // namespace rococo

