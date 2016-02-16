#pragma once

#include "../tpcc/piece.h"

namespace rococo {

class TpccRealDistPiece: public TpccPiece {
 public:
  using TpccPiece::TpccPiece;
  void RegNewOrder() override;
  void RegPayment() override;
  void RegDelivery() override;
  void RegOrderStatus() override;
  void RegStockLevel() override;
  virtual ~TpccRealDistPiece() { }
};

} // namespace rococo

