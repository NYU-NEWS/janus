//
// Created by chance_Lv on 2020/2/10.
//
#pragma once

#include <deptran/classic/scheduler.h>

namespace janus {
    class SchedulerAcc : public SchedulerClassic {
        using SchedulerClassic::SchedulerClassic;
    public:
        virtual bool DispatchPiece(Tx& tx,
                                   TxPieceData& cmd,
                                   TxnOutput& ret_output) override {
            SchedulerClassic::DispatchPiece(tx, cmd, ret_output);
            ExecutePiece(tx, cmd, ret_output);
            return true;
        }

        virtual bool Guard(Tx &tx_box, Row* row, int col_id, bool write=true)
        override {
            // do nothing.
            return true;
        }

        virtual bool DoPrepare(txnid_t tx_id) override {
            return false;
        }
    };
}
