#include "coordinator.h"
#include "ssid_predictor.h"
#include "bench/facebook/workload.h"
#include "bench/spanner/workload.h"
#include "bench/dynamic/workload.h"
#include "bench/tpcc/workload.h"

namespace janus {
    // std::unordered_map<i32, uint64_t> CoordinatorAcc::key_to_ts;
    std::unordered_map<parid_t, uint64_t> CoordinatorAcc::svr_to_ts;
    std::recursive_mutex CoordinatorAcc::mtx_{};

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
        int cnt = 0;

        auto cmds_by_par = txn->GetReadyPiecesData();
        // auto cmds_by_par = txn->GetReadyPiecesData(pieces_per_hop); // all ready pieces sent in one parallel round
        Log_debug("AccDispatchAsync for tx_id: %" PRIx64, txn->root_id_);
        // check if this txn is a single-shard txn: if so, always consistent and no need to validate
        bool is_single_shard = false;
        if (txn->AllPiecesDispatchable() && cmds_by_par.size() == 1) {
            is_single_shard = true;
        }
        // get a predicted ssid_spec
        uint64_t current_time = SSIDPredictor::get_current_time();

        // failure handling
        for (auto& pair : cmds_by_par) {
            if (tx_data().coord == UINT32_MAX) {
                // set coordinator
                tx_data().coord = pair.first;
            } else {
                // put into cohorts
                tx_data().cohorts.insert(pair.first);
            }
        }

        // TODO: use dispatched == 0 instead of ssid_spec == 0 to start the following block!
        if (tx_data().ssid_spec == 0) {
            // for multi-shot txns, we only do ssid prediction at the first shot and use the same ssid across shots
            std::vector<parid_t> servers;
            servers.reserve(cmds_by_par.size());
            for (auto& pair : cmds_by_par) {
                servers.emplace_back(pair.first);
            }
            tx_data().ssid_spec = current_time + SSIDPredictor::predict_ssid(servers);
        }
        // Log_info("Client:Dispatch. txid = %lu. current_time = %lu. sending ssid_spec = %lu.", txn->id_, current_time, tx_data().ssid_spec);
        for (auto& pair: cmds_by_par) {
            const parid_t& par_id = pair.first;
            auto& cmds = pair.second;
            n_dispatch_ += cmds.size();
            cnt += cmds.size();
            auto sp_vec_piece = std::make_shared<vector<shared_ptr<TxPieceData>>>();
            bool writing_to_par = false;
            bool write_only = true;
            tx_data().par_ids.insert(par_id);
            for (const auto& c: cmds) {
                /*
                Log_info("txid = %lu. Workload type = %d, op count = %d, keys_ size = %d. keys_ real size = %d. keys_ = %d. values size = %d. key = %d.",
                         txn->id_, c->root_type_, c->input.at(FB_OP_COUNT).get_i32(), c->input.size(), c->input.keys_.size(), *(c->input.keys_.begin()),
                         c->input.values_->size(), c->input.at(*(c->input.keys_.begin())).get_i32());
                */
                //Log_info("try this. roottype = %d; type = %d.", c->root_type_, c->type_);
                // for rotxn
                if (c->root_type_ == FB_ROTXN || c->root_type_ == SPANNER_ROTXN || c->root_type_ == TPCC_STOCK_LEVEL) {
                    tx_data().is_rotxn = true;
                    // i32 key = get_key(c);
                    // i64 ts = get_ts(key);
                    // c->input[TS_INDEX] = Value(ts);
                }
                // Log_info("workload type = %d. txn_id = %lu. key = %d.", c->root_type_, txn->id_, key);
                tx_data().innid_to_server[c->inn_id_] = par_id;
                tx_data().innid_to_starttime[c->inn_id_] = current_time;
                //Log_info("Client:Dispatch. txid = %lu. recording innid = %u. server = %u.", txn->id_, c->inn_id_, par_id);
                c->id_ = next_pie_id();
                c->op_type_ = txn_reg_->get_optype(c->root_type_, c->type_);
                if (!txn_reg_->get_write_only(c->root_type_, c->type_)) {
                    write_only = false;  // write-only if all pieces for this parid are write-only pieces
                }
                // c->write_only_ = txn_reg_->get_write_only(c->root_type_, c->type_);
                if (c->op_type_ == WRITE_REQ && !(write_only && is_single_shard)) {
                    writing_to_par = true;
                }
                //Log_info("try this. optype = %d.", c->op_type_);
                dispatch_acks_[c->inn_id_] = false;
                sp_vec_piece->push_back(c);
            }
 //           if (writing_to_par) {
                tx_data().pars_to_finalize.insert(par_id);
 //           }
            /*
            if (is_single_shard && write_only) {
                tx_data().n_single_shard_write_only++; // stats
            }
            */
            if (tx_data().is_rotxn) { // rotxn
                commo()->AccBroadcastRotxnDispatch(this->coo_id_,
                                                   sp_vec_piece,
                                                   this,
                                                   tx_data().ssid_spec,
                                                   get_ts(par_id),
                                                   std::bind(&CoordinatorAcc::AccDispatchAck,
                                                             this,
                                                             phase_,
                                                             std::placeholders::_1,
                                                             std::placeholders::_2,
                                                             std::placeholders::_3,
                                                             std::placeholders::_4,
                                                             std::placeholders::_5,
                                                             std::placeholders::_6,
                                                             std::placeholders::_7,
                                                             std::placeholders::_8));
            } else { // read-write txn
                commo()->AccBroadcastDispatch(this->coo_id_,
                                              sp_vec_piece,
                                              this,
                                              tx_data().ssid_spec,
                                              is_single_shard && write_only,
                                              tx_data().coord,
                                              tx_data().cohorts,
                                              std::bind(&CoordinatorAcc::AccDispatchAck,
                                                        this,
                                                        phase_,
                                                        std::placeholders::_1,
                                                        std::placeholders::_2,
                                                        std::placeholders::_3,
                                                        std::placeholders::_4,
                                                        std::placeholders::_5,
                                                        std::placeholders::_6,
                                                        std::placeholders::_7,
                                                        std::placeholders::_8));
            }
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
                                        uint64_t arrival_time,
                                        uint8_t rotxn_okay,
                                        const std::pair<parid_t, uint64_t>& new_svr_ts) {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (phase != phase_) return;
        auto* txn = (TxData*) cmd_;
        n_dispatch_ack_ += outputs.size();
        bool track_arrival_time = true;
        // Log_info("rotxn_okay = %d; returned_ts size = %d, returned_ts[-1] = %lu.", rotxn_okay, returned_ts.size(), returned_ts.at(-1));
        // for rotxn update key_ts
        /*
        for (const auto& key_ts : returned_ts) {
            update_key_ts(key_ts.first, key_ts.second);
        }
        */
        parid_t server = new_svr_ts.first;
        uint64_t new_ts = new_svr_ts.second;
        update_key_ts(server, new_ts);

        if (!rotxn_okay) {
            tx_data().is_rotxn_okay = false;
        }

        for (auto& pair : outputs) {
            const innid_t& inn_id = pair.first;
            // verify(!dispatch_acks_.at(inn_id));
            dispatch_acks_[inn_id] = true;
            Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
                      n_dispatch_ack_, n_dispatch_, cmd_->id_, inn_id);
            txn->Merge(pair.first, pair.second);
            // record arrival times
            if (!track_arrival_time) {
                continue;
            }
            //(tx_data().innid_to_server.find(inn_id) != tx_data().innid_to_server.end());
            //verify(tx_data().innid_to_starttime.find(inn_id) != tx_data().innid_to_starttime.end());
            parid_t server = tx_data().innid_to_server[inn_id];
            uint64_t starttime = tx_data().innid_to_starttime[inn_id];
            uint64_t time_delta = arrival_time >= starttime ? arrival_time - starttime : 0;  // for clock skew (rare case)
            // Log_info("Client:ACK. txid = %lu. innid = %lu. server = %lu. time_delta = %lu.", txn->id_, inn_id, server, time_delta);
            SSIDPredictor::update_time_tracker(server, time_delta);
            track_arrival_time = false;
        }
        // store RPC returned information
        switch (res) {
            /*
            case BOTH_NEGATIVE:
                txn->_offset_invalid = true;
                txn->_decided = false;
                break;
            case OFFSET_INVALID:
                txn->_offset_invalid = true;
                break;
            */
            case EARLY_ABORT:
                txn->_early_abort = true;
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
                // Log_info("not_send statusquery");
                // all dependent writes are finalized
                txn->_status_query_done = true;
	        } else {
                // now we send statusquery rpc as optimization
                // Log_info("send statusquery");
                /*
                for (const auto& parid : tx_data().par_ids) {
                    tx_data().n_status_query++;
                    commo()->AccBroadcastStatusQuery(parid,
                                                     tx_data().id_,
                                                     std::bind(&CoordinatorAcc::AccStatusQueryAck,
                                                               this,
                                                               tx_data().id_,
                                                               std::placeholders::_1));
                }
                */
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
    /*
    void CoordinatorAcc::AccStatusQueryAck(txnid_t tid, int8_t res) {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (tid != tx_data().id_) return;  // the queried txn has early returned
        if (is_frozen()) return;  // this txn has been decided, either committed or aborted
        if (tx_data()._status_query_done) return; // no need to wait on query acks, it has been decided
        tx_data().n_status_callback++;
        // Log_info("tx: %lu, received 1 Query ack. res = %d", tx_data().id_, res);
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
            // Log_info("tx: %lu, time as usual. commit = %d", tx_data().id_, committed_);
            if (phase_ % n_phase == Phase::INIT_END) { // this txn is waiting in FINAL phase
                // Log_info("tx: %lu, time goes back. commit = %d", tx_data().id_, committed_);
                time_flies_back();
            }
            // this txn has not got to FINAL phase yet, either validating or dispatching
        }
    }
    */
    // ------------- SAFEGUARD ----------------
    void CoordinatorAcc::SafeGuardCheck() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        // verify(!committed_);
        // ssid check consistency
        // added offset-1 optimization
        /*
        if (offset_1_check_pass() && !tx_data()._is_consistent) {
            // tx_data().n_offset_valid_++;  // for stats
        }
        */
        // if (tx_data()._is_consistent || offset_1_check_pass()) {
        if (tx_data()._early_abort || (tx_data().is_rotxn && !tx_data().is_rotxn_okay)) {
            committed_ = false;
            aborted_ = true;
            GotoNextPhase();
            return;
        }
        if (tx_data()._is_consistent) {
            tx_data()._is_consistent = true;
            committed_ = true;
            // tx_data().n_ssid_consistent_++;   // for stats
            n_ssid_consistent_++;   // for stats
        }
        if (committed_ || aborted_) { // SG checks consistent or cascading aborts, no need to validate
            //Log_info("tx: %lu, safeguard check, commit = %d; aborted = %d", tx_data().id_, committed_, aborted_);
            GotoNextPhase();
        } else {
            AccValidate();
        }
    }

    /*
    bool CoordinatorAcc::offset_1_check_pass() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        return !tx_data()._offset_invalid &&  // write valid
               tx_data().highest_ssid_low - tx_data().lowest_ssid_high <= 1;  // offset within 1
    }
    */

    void CoordinatorAcc::AccValidate() {
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        auto pars = tx_data().GetPartitionIds();
        // verify(!pars.empty());
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
            tx_data()._validation_failed = true; // stats
        }
        if (tx_data().n_validate_ack_ == tx_data().n_validate_rpc_) {
            // have received all acks
            // Log_info("tx: %lu, all validation ACKs received.", tx_data().id_);
            committed_ = !aborted_;
            if (!tx_data()._validation_failed) {
                // tx_data().n_validation_passed++;  // stats
                n_validation_passed++;  // stats
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
            AccAbort();
        }
    }

    void CoordinatorAcc::AccCommit() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        if (tx_data()._decided || tx_data()._status_query_done) {
            freeze_coordinator();
            //Log_info("tx: %lu, fast commit.", tx_data().id_);
            if (!tx_data().pars_to_finalize.empty() || tx_data()._is_consistent) { // for ss, if didn't do validation, we need to finalize reads
                AccFinalize(FINALIZED);
            }
            if (tx_data()._decided) {
                // tx_data().n_decided_++; // stats
                n_decided_++; // stats
            }
            End();  // do not wait for finalize to return, respond to user now
        } else {
            //Log_info("tx: %lu, wait for decision.", tx_data().id_);
            // do nothing, waiting in this phase for AccStatusQuery responses
        }
    }

    void CoordinatorAcc::AccAbort() {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
        //Log_info("tx: %lu, abort.", tx_data().id_);
        // (aborted_);
        freeze_coordinator();
        // abort as long as inconsistent or there is at least one statusquery ack is abort (cascading abort)
        // for stats
        if (tx_data()._early_abort) {
            // tx_data().n_early_aborts++;
            n_early_aborts++;
        }
        if (!tx_data()._early_abort && (tx_data()._is_consistent || !tx_data()._validation_failed)) {
            // this txn aborts due to cascading aborts, consistency check passed
            // tx_data().n_cascading_aborts++;
            if (tx_data()._status_abort) {
                n_cascading_aborts++;
            }
        }
        if (!tx_data().pars_to_finalize.empty() || tx_data()._is_consistent) {
            AccFinalize(ABORTED);  // will abort in AccFinalizeAck
        } else {
            // abort here
            Restart();
        }
    }

    void CoordinatorAcc::AccFinalize(int8_t decision) {
        // TODO: READS NO NEED TO FINALIZE
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        // Log_info("tx: %lu, sending finalizeRPC. decision = %d.", tx_data().id_, decision);
        // auto pars = tx_data().GetPartitionIds();

        /* for testing failure mode
        if (tx_data().id_ % 1000 == 438) {
            // Log_info("Client injects failure on tx: %lu. decision = %d.", tx_data().id_, decision);
            return;
        }
        */
        if (tx_data().is_rotxn) {
            // now rotxns do not finalize
            verify(!tx_data()._early_abort);
            if (tx_data()._is_consistent && !tx_data()._status_abort && decision == ABORTED) {
                n_rotxn_aborts++;
            }
            if (decision == ABORTED) {
                Restart();
            }
            return;
        }

        std::set<uint32_t> pars;
        if (tx_data()._is_consistent) {
            pars = tx_data().GetPartitionIds();
        } else {
            pars = tx_data().pars_to_finalize;
        }
        // verify(!pars.empty());
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
            // Log_info("tx: %lu, all ABORT finalize acks received.", tx_data().id_);
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
        // tx_data()._offset_invalid = false;
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
        tx_data().pars_to_finalize.clear();
        // falure handling
        tx_data().coord = UINT32_MAX;
        tx_data().cohorts.clear();
        tx_data().is_rotxn = false;
        tx_data().par_ids.clear();
        tx_data().is_rotxn_okay = true;
        tx_data()._early_abort = false;
    }

    /*
    i32 CoordinatorAcc::get_key(const shared_ptr<SimpleCommand>& c) {
        std::lock_guard<std::recursive_mutex> lock(this->mtx_);
//                Log_info("Workload type = %d, op count = %d, keys_ size = %d. keys_ real size = %d. keys_ = %d. values size = %d. key = %d.",
//                         c->root_type_, c->input.at(FB_OP_COUNT).get_i32(), c->input.size(), c->input.keys_.size(), *(c->input.keys_.begin()),
//                         c->input.values_->size(), c->input.at(*(c->input.keys_.begin())).get_i32());
//
        int workload = c->root_type_;
        // TODO: we only implement for facebook and spanner for now. TPCC comes later?
        switch(workload) {
            case FB_ROTXN: { //  this is a rotxn in FB, FB input.keys_ only store 1 key per command
                int key_index = *(c->input.keys_.begin());
                return c->input.at(key_index).get_i32();
            }
            case SPANNER_ROTXN: { // this is a rotxn in Spanner, Spanner keys_ store 1 key, a SPANNER_TXN_SIZE, and a SPANNER_RW_W_COUNT
                int key_index = *(c->input.keys_.begin());
                return c->input.at(key_index).get_i32();
            }
            default:
                return -1;
        }
    }
    */

    /*
    uint64_t CoordinatorAcc::get_ts(i32 key) {
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        if (key_to_ts.find(key) == key_to_ts.end()) {
            return 0;
        }
        return key_to_ts.at(key);
    }
    */

    /*
    void CoordinatorAcc::update_key_ts(i32 key, uint64_t new_ts) {
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        key_to_ts[key] = new_ts;
    }
    */

    uint64_t CoordinatorAcc::get_ts(uint32_t server) {
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        if (svr_to_ts.find(server) == svr_to_ts.end()) {
            return 0;
        }
        return svr_to_ts.at(server);
    }

    void CoordinatorAcc::update_key_ts(uint32_t server, uint64_t new_ts) {
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        if (svr_to_ts.find(server) != svr_to_ts.end() && svr_to_ts[server] >= new_ts) {
            return;
        }
        svr_to_ts[server] = new_ts;
    }
} // namespace janus
