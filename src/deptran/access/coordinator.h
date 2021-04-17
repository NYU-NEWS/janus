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
                            map<innid_t, map<int32_t, Value>>& outputs,
                            uint64_t arrival_time,
                            uint8_t rotxn_okay,
                            const std::pair<parid_t, uint64_t>& new_svr_ts);
        void SafeGuardCheck();
        void AccValidate();
        void AccValidateAck(phase_t phase, int8_t res);
        void AccFinish();
        void AccFinalize(int8_t decision);
	    void AccFinalizeAck(phase_t phase);
        void Restart() override;
        void reset_all_members();
        // bool offset_1_check_pass();
        void AccCommit();
        void AccAbort();
        void StatusQuery();
        void AccStatusQueryAck(txnid_t tid, int8_t res);
        void SkipDecidePhase();

        // for rotxn
        static std::unordered_map<parid_t, uint64_t> svr_to_ts;
        // static uint64_t get_ts(i32 key);
        static uint64_t get_ts(parid_t server);
        // i32 get_key(const shared_ptr<SimpleCommand>& c);
        // void update_key_ts(i32 key, uint64_t new_ts);
        static void update_key_ts(parid_t server, uint64_t new_ts);
    private:
        static std::recursive_mutex mtx_;
        bool finished = true;
        void freeze_coordinator() { verify(!finished); finished = true; }
        void unfreeze_coordinator() { verify(finished); finished = false; }
        bool is_frozen() const { return finished; }
        void time_flies_back() { phase_--; GotoNextPhase(); }
    };
} // namespace janus
