#include "coordinator.h"

namespace janus {
    AccCommo* CoordinatorAcc::commo() {
        if (commo_ == nullptr) {
            commo_ = new AccCommo();
        }
        auto acc_commo = dynamic_cast<AccCommo*>(commo_);
        verify(acc_commo != nullptr);
        return acc_commo;
    }

    void CoordinatorAcc::GotoNextPhase() {
        const int n_phase = 4;  // TODO: make this 4 for real
        switch (phase_++ % n_phase) {
            case Phase::INIT_END: verify(phase_ % n_phase == Phase::DISPATCH);
                DispatchAsync();
                break;
            case Phase::DISPATCH: verify(phase_ % n_phase == Phase::VALIDATE);
                // TODO: safeguard check. If consistent, respond.
                if (_is_consistent) { // SG says this txn is consistent
                    // return to the end user immediately and finish
                    committed_ = true;
                }
                GotoNextPhase(); // go validate if necessary
                //End();
                break;
            case Phase::VALIDATE: verify(phase_ % n_phase == Phase::DECIDE);
                // validate if _validate_abort != true and inconsistent
                if (!committed_ && _validate_abort) {
                    AccValidate();
//                    aborted_ = true; // TODO: no need to validate for now, later may apply some optimizations
                                     // TODO: validate or not in this case is an ML control knob
//                    GotoNextPhase();
                } else if (committed_) { // SG checks consistent, no need to validate
                    GotoNextPhase();
                } else {
                    AccValidate();
                }
                break;
            case Phase::DECIDE: verify(phase_ % n_phase == Phase::INIT_END);
                if (committed_) {
                    AccFinalize(FINALIZED);
                    End();
                } else if (aborted_) {
                    // TODO: do we need to update txns to be aborted on servers? could be an ML knob?
                    // TODO: seems no need for now, but might be needed when interacting with 2PL?
                    Restart();
                } else {
                    verify(0);
                }
                break;
            default:
                verify(0);  // invalid phase
                break;
        }
    }

    void CoordinatorAcc::DispatchAsync() {
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        auto txn = (TxData*) cmd_;

        int cnt = 0;
        auto cmds_by_par = txn->GetReadyPiecesData(1);
        Log_debug("AccDispatchAsync for tx_id: %" PRIx64, txn->root_id_);
        for (auto& pair: cmds_by_par) {
            const parid_t& par_id = pair.first;
            auto& cmds = pair.second;
            n_dispatch_ += cmds.size();
            cnt += cmds.size();
            auto sp_vec_piece = std::make_shared<vector<shared_ptr<TxPieceData>>>();
            for (const auto& c: cmds) {
                c->id_ = next_pie_id();
                dispatch_acks_[c->inn_id_] = false;
                sp_vec_piece->push_back(c);
            }
            commo()->AccBroadcastDispatch(sp_vec_piece,
                                     this,
                                          std::bind(&CoordinatorAcc::AccDispatchAck,
                                                 this,
                                                    phase_,
                                                    std::placeholders::_1,
                                                    std::placeholders::_2,
                                                    std::placeholders::_3,
                                                    std::placeholders::_4,
                                                    std::placeholders::_5));
        }
        Log_debug("AccDispatchAsync cnt: %d for tx_id: %" PRIx64, cnt, txn->root_id_);
    }

    void CoordinatorAcc::AccDispatchAck(phase_t phase,
                                        int res,
                                        uint64_t ssid_low,
                                        uint64_t ssid_high,
                                        uint64_t ssid_highest,
                                        map<innid_t, map<int32_t, Value>>& outputs) {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (phase != phase_) return;
        auto* txn = (TxData*) cmd_;
        n_dispatch_ack_ += outputs.size();
        for (auto& pair : outputs) {
            const innid_t& inn_id = pair.first;
            verify(!dispatch_acks_.at(inn_id));
            dispatch_acks_[inn_id] = true;
            Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
                      n_dispatch_ack_, n_dispatch_, cmd_->id_, inn_id);
            txn->Merge(pair.first, pair.second);
        }
        // store RPC returned information
        if (ssid_low > ssid_high) {  // no overlapped range
            _is_consistent = false;
        }
        if (res == VALIDATE_ABORT) {
            _validate_abort = true;
        }
        if (_is_consistent) { // if some ack has shown inconsistent, then no need to update these
            if (ssid_low > highest_ssid_low) {
                highest_ssid_low = ssid_low;
            }
            if (ssid_high < lowest_ssid_high) {
                lowest_ssid_high = ssid_high;
            }
        }
        if (ssid_highest > highest_ssid_high) {
            highest_ssid_high = ssid_highest;
        }

        if (txn->HasMoreUnsentPiece()) {
            Log_debug("command has more sub-cmd, cmd_id: %llx,"
                      " n_started_: %d, n_pieces: %d",
                      txn->id_, txn->n_pieces_dispatched_, txn->GetNPieceAll());
            this->DispatchAsync();
        } else if (AllDispatchAcked()) {
            Log_debug("receive all start acks, txn_id: %llx; START PREPARE", txn->id_);
            GotoNextPhase();
        }
    }

    void CoordinatorAcc::AccValidate() {
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        auto pars = tx_data().GetPartitionIds();
        verify(!pars.empty());
        for (auto par_id : pars) {
            n_validate_rpc_++;
            commo()->AccBroadcastValidate(par_id,
                                          tx_data().id_,
                                          highest_ssid_high,
                                          std::bind(&CoordinatorAcc::AccValidateAck,
                                                          this,
                                                             phase_,
                                                             std::placeholders::_1));
        }
    }

    void CoordinatorAcc::AccValidateAck(phase_t phase, int8_t res) {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (phase != phase_) return;
        n_validate_ack_++;
        if (res == INCONSISTENT) { // at least one shard returns inconsistent
            aborted_ = true;
        }
        if (n_validate_ack_ == n_validate_rpc_) {
            // have received all acks
            committed_ = !aborted_;
            if (!committed_) {
                int x = 0;
            }
            // -------Testing purpose here----------
            // TODO: take this out
            //aborted_ = false;
            //committed_ = true;
            //--------------------------------------
            GotoNextPhase(); // to phase decide
            return;
        }
    }

    void CoordinatorAcc::AccFinalize(int8_t decision) {
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        auto pars = tx_data().GetPartitionIds();
        verify(!pars.empty());
        for (auto par_id : pars) {
            commo()->AccBroadcastFinalize(par_id,
                                          tx_data().id_,
                                          decision);
        }
    }

    void CoordinatorAcc::Restart() {
        reset_all_members();
        CoordinatorClassic::Restart();
    }

    void CoordinatorAcc::reset_all_members() {
        _is_consistent = true;
        _validate_abort = false;
        highest_ssid_low = 0;
        lowest_ssid_high = UINT_MAX;
        highest_ssid_high = 0;
        n_validate_rpc_ = 0;
        n_validate_ack_ = 0;
    }
} // namespace janus