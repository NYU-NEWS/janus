//
// Created by chance_Lv on 2020/2/3.
//
#pragma once

#include "deptran/classic/coordinator.h"
#include "commo.h"

namespace janus {
    class CoordinatorAcc : public CoordinatorClassic {
    public:
        using CoordinatorClassic::CoordinatorClassic;
        enum Phase {INIT_END=0, DISPATCH=1, VALIDATE=2, DECIDE=3};
        void GotoNextPhase() override;
        void DispatchAsync() override;
        AccCommo* commo();
        void AccDispatchAck(phase_t phase,
                            int res,
                            uint64_t ssid_min,
                            uint64_t ssid_max,
                            map<innid_t, map<int32_t, Value>>& outputs);
        void SafeGuardCheck();
        void AccValidate();
        void AccValidateAck(phase_t phase, int8_t res);
        void AccFinalizeNoWait(int8_t decision);
        void AccFinalize(int8_t decision);
        void AccFinalizeAck(phase_t phase, int8_t res);
        void Restart() override;
        void reset_all_members();
        bool offset_1_check_pass();
        void AccCommit();
    };
} // namespace janus
