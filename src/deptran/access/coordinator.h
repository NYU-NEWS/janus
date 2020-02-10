//
// Created by chance_Lv on 2020/2/3.
//
#pragma once

#include "deptran/classic/coordinator.h"

namespace janus {
    class CoordinatorAcc : public CoordinatorClassic {
    public:
        using CoordinatorClassic::CoordinatorClassic;
        enum Phase {INIT_END=0, DISPATCH=1};
        void GotoNextPhase() override;
    };
} // namespace janus
