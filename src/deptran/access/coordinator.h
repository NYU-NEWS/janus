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
	    const int n_phase = 3;
        enum Phase {INIT_END=0, SAFEGUARD=1, FINISH=2};
        void GotoNextPhase() override;
        void DispatchAsync() override;
        AccCommo* commo();
        void AccDispatchAck(phase_t phase,
                            int res,
                            uint64_t ssid_low,
                            uint64_t ssid_high,
                            uint64_t ssid_new,
                            map<innid_t, map<int32_t, Value>>& outputs);
        void SafeGuardCheck();
        void AccValidate();
        void AccValidateAck(phase_t phase, int8_t res);
        void AccFinish();
        void AccFinalize(int8_t decision);
	    void AccFinalizeAck(phase_t phase);
        void Restart() override;
        void reset_all_members();
        bool offset_1_check_pass();
        void AccCommit();
        void AccAbort();
        void StatusQuery();
        void AccStatusQueryAck(txnid_t tid, int8_t res);
        void SkipDecidePhase();
    private:
        bool finished = true;
        void freeze_coordinator() { verify(!finished); finished = true; }
        void unfreeze_coordinator() { verify(finished); finished = false; }
        bool is_frozen() const { return finished; }
        void time_flies_back() { phase_--; GotoNextPhase(); }
    };
} // namespace janus
