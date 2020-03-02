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
        switch (phase_++ % n_phase) {
            case Phase::INIT_END: verify(phase_ % n_phase == Phase::DISPATCH);
                DispatchAsync();
                StatusQuery();
                break;
            case Phase::DISPATCH: verify(phase_ % n_phase == Phase::VALIDATE);
                SafeGuardCheck();
                break;
            case Phase::VALIDATE: verify(phase_ % n_phase == Phase::DECIDE);
                if (committed_) { // SG checks consistent, no need to validate
                    GotoNextPhase();
                } else {
                    AccValidate();
                }
                break;
            case Phase::DECIDE: verify(phase_ % n_phase == Phase::DECIDING);
                if (committed_) {
                    AccCommit();
                    // reset_all_members();  tx_data is GC'ed in End()
                } else if (aborted_) {
                    AccAbort();
                } else {
                    verify(0);
                }
                break;
            case Phase::DECIDING: verify(phase_ % n_phase == Phase::INIT_END);
                // do nothing here, will called in StatusQueryAck
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
                                          tx_data().ssid_spec,
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
                                        uint64_t ssid_new,
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
        switch (res) {
            case BOTH_NEGATIVE:
                txn->_offset_invalid = true;
                txn->_decided = false;
                break;
            case OFFSET_INVALID:
                txn->_offset_invalid = true;
                break;
            case NOT_DECIDED:
                txn->_decided = false;
                break;
            default:  // SUCCESS, do nothing
                break;
        }
        if (ssid_new > txn->ssid_new) {
            txn->ssid_new = ssid_new;
        }
        if (ssid_low > txn->highest_ssid_low) {
            txn->highest_ssid_low = ssid_low;
        }
        if (ssid_high < txn->lowest_ssid_high) {
            txn->lowest_ssid_high = ssid_high;
        }
        // basic ssid check consistency
        if (txn->highest_ssid_low > txn->lowest_ssid_high) {
            // inconsistent if no overlapped range
            txn->_is_consistent = false;
        }
        if (txn->HasMoreUnsentPiece()) {
            Log_debug("command has more sub-cmd, cmd_id: %llx,"
                      " n_started_: %d, n_pieces: %d",
                      txn->id_, txn->n_pieces_dispatched_, txn->GetNPieceAll());
            this->DispatchAsync();
        } else if (AllDispatchAcked()) {
            Log_debug("receive all start acks, txn_id: %llx; START PREPARE", txn->id_);
            if (txn->_decided) {
                // all dependent writes are finalized
                txn->_status_query_done = true;  // do not need AccQueryStatus acks
            }
            GotoNextPhase();
        }
    }

    void CoordinatorAcc::AccValidate() {
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        auto pars = tx_data().GetPartitionIds();
        verify(!pars.empty());
        tx_data()._decided = true;  // clear decided from dispatch,
                                    // not-decided in dispatch could be now decided after validation
        for (auto par_id : pars) {
            tx_data().n_validate_rpc_++;
            commo()->AccBroadcastValidate(par_id,
                                          tx_data().id_,
                                          tx_data().ssid_new,   // the new cross-shard ssid for writes
                                          std::bind(&CoordinatorAcc::AccValidateAck,
                                                          this,
                                                             phase_,
                                                             std::placeholders::_1));
        }
    }

    void CoordinatorAcc::AccValidateAck(phase_t phase, int8_t res) {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (phase != phase_) return;
        tx_data().n_validate_ack_++;
        if (res == INCONSISTENT) { // at least one shard returns inconsistent
            aborted_ = true;
        }
        if (res == NOT_DECIDED) {  // consistent but not decided
            tx_data()._decided = false;
        }
        if (tx_data().n_validate_ack_ == tx_data().n_validate_rpc_) {
            // have received all acks
            committed_ = !aborted_;
            if (tx_data()._decided) {
                tx_data()._status_query_done = true; // no need to wait for AccStatusQuery acks
            }
            // -------Testing purpose here----------
            //aborted_ = false;
            //committed_ = true;
            //--------------------------------------
            GotoNextPhase(); // to phase decide
        }
    }

    void CoordinatorAcc::AccFinalize(int8_t decision) {
        // TODO: READS NO NEED TO FINALIZE
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
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        cmd_->root_id_ = this->next_txn_id();
        cmd_->id_ = cmd_->root_id_;
        reset_all_members();
        CoordinatorClassic::Restart();
    }

    void CoordinatorAcc::reset_all_members() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        tx_data()._is_consistent = true;
        tx_data().highest_ssid_low = 0;
        tx_data().lowest_ssid_high = UINT64_MAX;
        tx_data().ssid_new = 0;
        tx_data().n_validate_rpc_ = 0;
        tx_data().n_validate_ack_ = 0;
        tx_data()._offset_invalid = false;
        tx_data().ssid_spec = 0;
        tx_data()._decided = true;
        tx_data()._status_query_done = false;
        tx_data()._status_abort = false;
        tx_data().n_status_query = 0;
        tx_data().n_status_callback = 0;
    }

    void CoordinatorAcc::SafeGuardCheck() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        verify(!committed_);
        // ssid check consistency
        // added offset-1 optimization
        if (tx_data()._is_consistent || offset_1_check_pass()) {
            committed_ = true;
            tx_data().n_ssid_consistent_++;   // for stats
        }
        if (offset_1_check_pass() && !tx_data()._is_consistent) {
            tx_data().n_offset_valid_++;  // for stats
        }
        GotoNextPhase(); // go validate if necessary
    }

    bool CoordinatorAcc::offset_1_check_pass() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        return !tx_data()._offset_invalid &&  // write valid
               tx_data().highest_ssid_low - tx_data().lowest_ssid_high <= 1;  // offset within 1
    }

    void CoordinatorAcc::AccCommit() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (tx_data()._decided || tx_data()._status_query_done) {
            AccFinalize(FINALIZED);
            if (tx_data()._decided) {
                tx_data().n_decided_++; // stats
            }
            End();  // do not wait for finalize to return, respond to user now
        }
        if (phase_ % n_phase == Phase::DECIDING) { // current is DECIDE phase
            GotoNextPhase();
        }
    }

    void CoordinatorAcc::AccAbort() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        verify(aborted_);
        // abort as long as inconsistent or there is at least one statusquery ack is abort (cascading abort)
        AccFinalize(ABORTED);
        Restart();
        if (phase_ % n_phase == Phase::DECIDING) { // current is DECIDE phase
            GotoNextPhase();
        }
    }

    void CoordinatorAcc::StatusQuery() {
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        auto pars = tx_data().GetPartitionIds();
        verify(!pars.empty());
        for (auto par_id : pars) {
            tx_data().n_status_query++;
            commo()->AccBroadcastStatusQuery(par_id,
                                             tx_data().id_,
                                             std::bind(&CoordinatorAcc::AccStatusQueryAck,
                                                             this,
                                                                std::placeholders::_1));
        }
    }

    void CoordinatorAcc::AccStatusQueryAck(int8_t res) {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (tx_data()._status_query_done) {
            // no need to wait on query acks, it has been decided
            return;
        }
        tx_data().n_status_callback++;
        switch (res) {
            case FINALIZED:  // do nothing
                break;
            case ABORTED:
                tx_data()._status_abort = true;
                break;
            default: verify(0); break;
        }
        if (tx_data()._status_abort) {
            // TODO: may early abort
            committed_ = false;
            aborted_ = true;
            tx_data()._status_query_done = true;
            AccAbort();
        }
        if (tx_data().n_status_query == tx_data().n_status_callback) { // received all callbacks, and thus res=finalized
            tx_data()._status_query_done = true;
            if (phase_ % n_phase == Phase::INIT_END) { // this txn is in DECIDING phase
                // finalize this txn, commit if consistent, abort if not, replaying the DECIDE phase
                if (committed_) {
                    AccCommit();
                } else {
                    AccAbort();
                }
            }
            // this txn has not got to DECIDE phase yet, either validating or dispatching
        }
    }
} // namespace janus
