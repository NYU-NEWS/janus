#include "coordinator.h"
#include "ssid_predictor.h"
#include "bench/spanner/workload.h"
#include "bench/dynamic/workload.h"

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
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        switch (phase_++ % n_phase) {
            case Phase::INIT_END: verify(phase_ % n_phase == Phase::SAFEGUARD);
                unfreeze_coordinator();
                DispatchAsync();
                break;
            case Phase::SAFEGUARD: verify(phase_ % n_phase == Phase::FINISH);
                SafeGuardCheck();
                break;
            case Phase::FINISH: verify(phase_ % n_phase == Phase::INIT_END);
                AccFinish();
                break;
            default:
                verify(0);  // invalid phase
                break;
        }
    }

    // -----------------INIT_END------------------
    void CoordinatorAcc::DispatchAsync() {
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        auto txn = (TxData*) cmd_;
        //Log_info("try this: %d", txn_reg_->get(1, 1000).input_vars_.size());
        int cnt = 0;
        int pieces_per_hop = 0;
        if (cmd_->type_ == SPANNER_RW && tx_data().n_pieces_dispatch_acked_ == 0) {
            // we make spanner RW txn multi-hop!
            pieces_per_hop = cmd_->spanner_rw_reads;
        }
        if (cmd_->type_ == DYNAMIC_RW && tx_data().n_pieces_dispatch_acked_ == 0) {
            // we make dynamic RW txn multi-hop!
            pieces_per_hop = cmd_->dynamic_rw_reads;
        }

        auto cmds_by_par = txn->GetReadyPiecesData(pieces_per_hop); // all ready pieces sent in one parallel round
        Log_debug("AccDispatchAsync for tx_id: %" PRIx64, txn->root_id_);
        // check if this txn is a single-shard txn: if so, always consistent and no need to validate
        if (txn->AllPiecesDispatchable() && cmds_by_par.size() == 1) {
            // tx_data()._single_shard_txn = true;
            // tx_data().n_single_shard++;  // stats
        }
        // get a predicted ssid_spec
        uint64_t current_time = SSIDPredictor::get_current_time();
        if (tx_data().ssid_spec == 0) {
            // for multi-shot txns, we only do ssid prediction at the first shot and use the same ssid across shots
            std::vector<parid_t> servers;
            servers.reserve(cmds_by_par.size());
            for (auto& pair : cmds_by_par) {
                servers.emplace_back(pair.first);
            }
            tx_data().ssid_spec = current_time + SSIDPredictor::predict_ssid(servers);
        }
        //Log_info("Client:Dispatch. txid = %lu. current_time = %lu. sending ssid_spec = %lu.", txn->id_, current_time, tx_data().ssid_spec);
        for (auto& pair: cmds_by_par) {
            const parid_t& par_id = pair.first;
            auto& cmds = pair.second;
            n_dispatch_ += cmds.size();
            cnt += cmds.size();
            auto sp_vec_piece = std::make_shared<vector<shared_ptr<TxPieceData>>>();
            bool writing_to_par = false;
            bool write_only = true;
            for (const auto& c: cmds) {
                //Log_info("try this. roottype = %d; type = %d.", c->root_type_, c->type_);
                tx_data().innid_to_server[c->inn_id_] = par_id;
                tx_data().innid_to_starttime[c->inn_id_] = current_time;
                //Log_info("Client:Dispatch. txid = %lu. recording innid = %u. server = %u.", txn->id_, c->inn_id_, par_id);
                c->id_ = next_pie_id();
                c->op_type_ = txn_reg_->get_optype(c->root_type_, c->type_);
                if (!txn_reg_->get_write_only(c->root_type_, c->type_)) {
                    write_only = false;  // write-only if all pieces for this parid are write-only pieces
                }
                // c->write_only_ = txn_reg_->get_write_only(c->root_type_, c->type_);
                if (c->op_type_ == WRITE_REQ && !(write_only && tx_data()._single_shard_txn)) {
                    writing_to_par = true;
                }
                //Log_info("try this. optype = %d.", c->op_type_);
                dispatch_acks_[c->inn_id_] = false;
                sp_vec_piece->push_back(c);
            }
            if (writing_to_par) {
                tx_data().pars_to_finalize.insert(par_id);
            }
            if (tx_data()._single_shard_txn && write_only) {
                tx_data().n_single_shard_write_only++; // stats
            }
            commo()->AccBroadcastDispatch(sp_vec_piece,
                                          this,
                                          tx_data().ssid_spec,
                                          tx_data()._single_shard_txn,
                                          write_only,
                                          std::bind(&CoordinatorAcc::AccDispatchAck,
                                                 this,
                                                    phase_,
                                                    std::placeholders::_1,
                                                    std::placeholders::_2,
                                                    std::placeholders::_3,
                                                    std::placeholders::_4,
                                                    std::placeholders::_5,
                                                    std::placeholders::_6),
					                       tx_data().id_,
                                          tx_data().n_status_query,
                                          std::bind(&CoordinatorAcc::AccStatusQueryAck,
                                                    this,
                                                    tx_data().id_,
                                                    std::placeholders::_1)); 
        }
        //Log_info("DISPATCH txid = %lu.", tx_data().id_);
        Log_debug("AccDispatchAsync cnt: %d for tx_id: %" PRIx64, cnt, txn->root_id_);
    }

    void CoordinatorAcc::AccDispatchAck(phase_t phase,
                                        int res,
                                        uint64_t ssid_low,
                                        uint64_t ssid_high,
                                        uint64_t ssid_new,
                                        map<innid_t, map<int32_t, Value>>& outputs,
                                        uint64_t arrival_time) {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (phase != phase_) return;
        auto* txn = (TxData*) cmd_;
        n_dispatch_ack_ += outputs.size();
        bool track_arrival_time = true;
        for (auto& pair : outputs) {
            const innid_t& inn_id = pair.first;
            verify(!dispatch_acks_.at(inn_id));
            dispatch_acks_[inn_id] = true;
            Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
                      n_dispatch_ack_, n_dispatch_, cmd_->id_, inn_id);
            txn->Merge(pair.first, pair.second);
            // record arrival times
            if (!track_arrival_time) {
                continue;
            }
            verify(tx_data().innid_to_server.find(inn_id) != tx_data().innid_to_server.end());
            verify(tx_data().innid_to_starttime.find(inn_id) != tx_data().innid_to_starttime.end());
            parid_t server = tx_data().innid_to_server.at(inn_id);
            uint64_t starttime = tx_data().innid_to_starttime.at(inn_id);
            uint64_t time_delta = arrival_time >= starttime ? arrival_time - starttime : 0;  // for clock skew (rare case)
            //Log_info("Client:ACK. txid = %lu. innid = %lu. server = %lu. time_delta = %lu.", txn->id_, inn_id, server, time_delta);
            SSIDPredictor::update_time_tracker(server, time_delta);
            track_arrival_time = false;
        }
        // store RPC returned information
        switch (res) {
            case EARLY_ABORT:
                txn->_early_abort = true;
                break;
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
        if (txn->highest_ssid_low > txn->lowest_ssid_high && !tx_data()._single_shard_txn) {
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
                // txn->_status_query_done = true;  // do not need AccQueryStatus acks
	        }
	        GotoNextPhase();
        }
    }

    /*
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
                                                       tx_data().id_,
                                                       std::placeholders::_1));
        }
    }
    */
    void CoordinatorAcc::AccStatusQueryAck(txnid_t tid, int8_t res) {
        verify(0);
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (tid != tx_data().id_) return;  // the queried txn has early returned
        if (is_frozen()) return;  // this txn has been decided, either committed or aborted
        if (tx_data()._status_query_done) return; // no need to wait on query acks, it has been decided
        tx_data().n_status_callback++;
        switch (res) {
            case FINALIZED:  // do nothing
                break;
            case ABORTED:
                tx_data()._status_abort = true;
                break;
            default: verify(0); break;
        }
        if (tx_data().n_status_query == tx_data().n_status_callback) { // received all callbacks
            tx_data()._status_query_done = true;
            if (tx_data()._status_abort) {
                aborted_ = true;
            }
            committed_ = !aborted_;
            //Log_info("tx: %lu, time as usual. commit = %d", tx_data().id_, committed_);
            if (phase_ % n_phase == Phase::INIT_END) { // this txn is waiting in FINAL phase
                //Log_info("tx: %lu, time goes back. commit = %d", tx_data().id_, committed_);
                time_flies_back();
            }
            // this txn has not got to FINAL phase yet, either validating or dispatching
        }
    }

    // ------------- SAFEGUARD ----------------
    void CoordinatorAcc::SafeGuardCheck() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        //Log_info("SG txid = %lu.", tx_data().id_);
        verify(!committed_);
        // ssid check consistency
        // added offset-1 optimization
        if (tx_data()._early_abort) {
            aborted_ = true;
        } else {
            if (offset_1_check_pass() && !tx_data()._is_consistent) {
                tx_data().n_offset_valid_++;  // for stats
            }
            if (tx_data()._is_consistent || offset_1_check_pass()) {
                tx_data()._is_consistent = true;
                committed_ = true;
                tx_data().n_ssid_consistent_++;   // for stats
            }
        }
        if (committed_ || aborted_) { // SG checks consistent or cascading aborts, no need to validate
            //Log_info("tx: %lu, safeguard check, commit = %d; aborted = %d", tx_data().id_, committed_, aborted_);
            GotoNextPhase();
        } else {
            AccValidate();
        }
    }

    bool CoordinatorAcc::offset_1_check_pass() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        return !tx_data()._offset_invalid &&  // write valid
               tx_data().highest_ssid_low - tx_data().lowest_ssid_high <= 1;  // offset within 1
    }

    void CoordinatorAcc::AccValidate() {
        verify(0);  // early-abort makes no validtion
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        auto pars = tx_data().GetPartitionIds();
        verify(!pars.empty());
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
        verify(0);  // early-abort makes no validtion
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (phase != phase_) return;
        tx_data().n_validate_ack_++;
        if (res == INCONSISTENT) { // at least one shard returns inconsistent
            aborted_ = true;
            tx_data()._validation_failed = true; // stats
        }
        if (tx_data().n_validate_ack_ == tx_data().n_validate_rpc_) {
            // have received all acks
            committed_ = !aborted_;
            if (!tx_data()._validation_failed) {
                tx_data().n_validation_passed++;  // stats
            }
           // Log_info("tx: %lu, acc_validate_ack. commit = %d", tx_data().id_, committed_);
            GotoNextPhase(); // to phase final
        }
    }

    // ------------ FINAL --------------
    void CoordinatorAcc::AccFinish() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (committed_) {
            AccCommit();
        } else if (aborted_) {
            AccAbort();
        } else {
            verify(0);
        }
    }

    void CoordinatorAcc::AccCommit() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        //if (tx_data()._decided || tx_data()._status_query_done) {
            freeze_coordinator();
            //Log_info("tx: %lu, fast commit.", tx_data().id_);
            if (!tx_data().pars_to_finalize.empty()) {
                AccFinalize(FINALIZED);
            }
            if (tx_data()._decided) {
                tx_data().n_decided_++; // stats
            }
            End();  // do not wait for finalize to return, respond to user now
        //} else {
            //Log_info("tx: %lu, wait for decision.", tx_data().id_);
            // do nothing, waiting in this phase for AccStatusQuery responses
        //}
    }

    void CoordinatorAcc::AccAbort() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        //Log_info("tx: %lu, abort.", tx_data().id_);
        verify(aborted_);
        freeze_coordinator();
        // abort as long as inconsistent or there is at least one statusquery ack is abort (cascading abort)
        // for stats
        if (tx_data()._early_abort) {
            tx_data().n_early_aborts++;
        }
        if (!tx_data()._early_abort && (tx_data()._is_consistent || !tx_data()._validation_failed)) {
            // this txn aborts due to cascading aborts, consistency check passed
            tx_data().n_cascading_aborts++;
        }
        if (!tx_data().pars_to_finalize.empty()) {
            AccFinalize(ABORTED);  // will abort in AccFinalizeAck
        } else {
            // abort here
            Restart();
        }
    }

    void CoordinatorAcc::AccFinalize(int8_t decision) {
        // TODO: READS NO NEED TO FINALIZE
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        // auto pars = tx_data().GetPartitionIds();
        auto pars = tx_data().pars_to_finalize;
        verify(!pars.empty());
	    switch (decision) {
            case FINALIZED:
                for (auto par_id : pars) {
                     commo()->AccBroadcastFinalize(par_id,
                                                  tx_data().id_,
                                                  decision);
                }
                break;
            case ABORTED:
                for (auto par_id : pars) {
                    tx_data().n_abort_sent++;
                    commo()->AccBroadcastFinalizeAbort(par_id,
                                                       tx_data().id_,
                                                       decision,
                                                       std::bind(&CoordinatorAcc::AccFinalizeAck,
                                                                 this,
                                                                 phase_));
                }
                break;
            default: verify(0); break;
        }
    }

    void CoordinatorAcc::AccFinalizeAck(phase_t phase) {
        // this is only called by Finalize(ABORTED)
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (phase != phase_) return;
        tx_data().n_abort_ack++;
        if (tx_data().n_abort_ack == tx_data().n_abort_sent) {
            // we only restart after this txn is aborted everywhere
            Restart();
        }
    }

    void CoordinatorAcc::Restart() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        cmd_->root_id_ = this->next_txn_id();
        cmd_->id_ = cmd_->root_id_;
        reset_all_members();
        CoordinatorClassic::Restart();
    }

    // --------- state clean ------------
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
	    tx_data().n_abort_sent = 0;
        tx_data().n_abort_ack = 0;
        tx_data().innid_to_server.clear();
        tx_data().innid_to_starttime.clear();
        tx_data()._validation_failed = false;
        tx_data()._early_abort = false;
        tx_data().pars_to_finalize.clear();
        tx_data()._single_shard_txn = false;
    }
} // namespace janus
