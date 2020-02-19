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
                            uint64_t ssid_low,
                            uint64_t ssid_high,
                            uint64_t ssid_highest,
                            map<innid_t, map<int32_t, Value>>& outputs);
        void AccValidate();
        void AccValidateAck(phase_t phase, int8_t res);
    private:
        bool _is_consistent = true;
        bool _validate_abort = false;
        // a consistent snapshot resides between highest_low and lowest_high
        snapshotid_t highest_ssid_low = 0;
        snapshotid_t lowest_ssid_high = UINT_MAX;
        snapshotid_t highest_ssid_high = 0;  // for updating ssids upon validation
                                             // and for final ssid if validation says consistent
        int n_validate_rpc_ = 0;
        int n_validate_ack_ = 0;
    };
} // namespace janus
