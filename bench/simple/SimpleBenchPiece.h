//
// Created by lamont on 12/28/15.
//
#pragma once
#include "deptran/piece.h"

namespace rococo {
  class SimpleBenchPiece : public Piece {
  public:
    virtual void reg_all() override;
  };
}
