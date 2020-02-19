//
// Created by chance_Lv on 2020/2/10.
//
#pragma once

#include <deptran/classic/scheduler.h>

namespace janus {
    class SchedulerAcc : public SchedulerClassic {
        using SchedulerClassic::SchedulerClassic;
    public:
        bool OnDispatch(cmdid_t cmd_id,
                        const shared_ptr<Marshallable>& cmd,
                        uint64_t* ssid_low,
                        uint64_t* ssid_high,
                        uint64_t* ssid_highest,
                        TxnOutput& ret_output);     // AccDispatch RPC handler
        bool Guard(Tx &tx_box, Row* row, int col_id, bool write) override {
            // do nothing, just has to override this pure virtual func.
            return true;
        }
    };
}
