//
// Created by Shuai on 6/25/17.
//

#pragma once

#include "../frame.h"

namespace janus {


class ExternCFrame : public Frame {
 public:
  ExternCFrame() : Frame(MODE_EXTERNC) { }
  TxLogServer *CreateScheduler() override;
};

} // namespace janus
