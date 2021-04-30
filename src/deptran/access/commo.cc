#include "commo.h"

namespace janus {
    void AccCommo::AccBroadcastDispatch(uint32_t coo_id,
                                        shared_ptr<vector<shared_ptr<SimpleCommand>>> sp_vec_piece,
                                        Coordinator *coo,
                                        snapshotid_t ssid_spec,
                                        bool is_single_shard_write_only,
                                        const std::function<void(int res,
                                                                 uint64_t ssid_low,
                                                                 uint64_t ssid_high,
                                                                 uint64_t ssid_new,
                                                                 TxnOutput &,
                                                                 uint64_t arrival_time,
                                                                 uint8_t rotxn_okay,
                                                                 const std::pair<parid_t, uint64_t>& new_svr_ts)> &callback) {
        cmdid_t cmd_id = sp_vec_piece->at(0)->root_id_;
        // verify(sp_vec_piece->size() > 0);
        auto par_id = sp_vec_piece->at(0)->PartitionId();
        rrr::FutureAttr fuattr;
        fuattr.callback =
                [coo, this, callback](Future* fu) {
                    int32_t ret;
                    uint64_t ssid_low;
                    uint64_t ssid_high;
                    uint64_t ssid_new;
                    TxnOutput outputs;
                    uint64_t arrival_time;
                    uint8_t rotxn_okay;
                    std::pair<parid_t, uint64_t> new_svr_ts;
                    fu->get_reply() >> ret >> ssid_low >> ssid_high >> ssid_new >> outputs >> arrival_time >> rotxn_okay >> new_svr_ts;
                    callback(ret, ssid_low, ssid_high, ssid_new, outputs, arrival_time, rotxn_okay, new_svr_ts);
                };
        auto pair_leader_proxy = LeaderProxyForPartition(par_id);
        Log_debug("send dispatch to site %ld",
                  pair_leader_proxy.first);
        auto proxy = pair_leader_proxy.second;
        shared_ptr<VecPieceData> sp_vpd(new VecPieceData);
        sp_vpd->sp_vec_piece_data_ = sp_vec_piece;
        MarshallDeputy md(sp_vpd); // ????
        rrr::Future* future;
        // if (par_id == coord) {
            // this is sending to the backup coordinator
            future = proxy->async_AccDispatch(coo_id, cmd_id, md, ssid_spec, (uint8_t)is_single_shard_write_only, fuattr);
        // } else {
            // this is sending to a cohort
        //    future = proxy->async_AccDispatch(coo_id, cmd_id, md, ssid_spec, (uint8_t)is_single_shard_write_only, coord, {}, fuattr);
        //}
        // auto future = proxy->async_AccDispatch(coo_id, cmd_id, md, ssid_spec, (uint8_t)is_single_shard_write_only, fuattr); // call Acc dispatch RPC
	    // now insert AccStatusQuery RPC here
	    /* we don't send status query rpc now, we do the optimization that only sending statusquery if it is not decided
        rrr::FutureAttr status_fuattr;
        status_fuattr.callback =
                [coo, this, callback_status](Future* fu) {
                    int8_t ret;
                    fu->get_reply() >> ret;
                    callback_status(ret);
                };
        n_status_query++;
        auto future_status = proxy->async_AccStatusQuery(status_cmd_id, status_fuattr); // call Acc StatusQuery RPC
        */
        // release both RPCs sequentially
        Future::safe_release(future);
	    // Future::safe_release(future_status);

        // FIXME fix this, this cause occ and perhaps 2pl to fail
        for (auto& pair : rpc_par_proxies_[par_id]) {
            if (pair.first != pair_leader_proxy.first) {
                rrr::FutureAttr fu2, fu2_status;
                fu2.callback =
                        [coo, this, callback](Future* fu) {
                            int32_t ret;
                            uint64_t ssid_low;
                            uint64_t ssid_high;
                            uint64_t ssid_new;
                            TxnOutput outputs;
                            uint64_t arrival_time;
                            uint8_t rotxn_okay;
                            std::pair<parid_t, uint64_t> new_svr_ts;
                            fu->get_reply() >> ret >> ssid_low >> ssid_high >> ssid_new >> outputs >> arrival_time >> rotxn_okay >> new_svr_ts;
                            // do nothing
                        };
                /*
		        fu2_status.callback =
                        [coo, this, callback_status](Future* fu) {
                            int8_t ret;
                            fu->get_reply() >> ret;
                            // do nothing
                        };
		        */
                // if (par_id == coord) {
                    Future::safe_release(pair.second->async_AccDispatch(coo_id, cmd_id, md, ssid_spec, (uint8_t)is_single_shard_write_only, fu2));
		        //} else {
                //    Future::safe_release(pair.second->async_AccDispatch(coo_id, cmd_id, md, ssid_spec, (uint8_t)is_single_shard_write_only, coord, {}, fu2));
		        //}
                // Future::safe_release(pair.second->async_AccDispatch(coo_id, cmd_id, md, ssid_spec, (uint8_t)is_single_shard_write_only, fu2));
		        // Future::safe_release(pair.second->async_AccStatusQuery(status_cmd_id, fu2_status));
            }
        }
    }

    void AccCommo::AccBroadcastValidate(parid_t par_id,
                                        cmdid_t cmd_id,
                                        snapshotid_t ssid_new,  // the new cross-shard ssid
                                        const std::function<void(int8_t res)> &callback) {
        rrr::FutureAttr fuattr;
        fuattr.callback =
                [this, callback](Future* fu) {
                    int8_t ret;
                    fu->get_reply() >> ret;
                    callback(ret);
                };
        auto pair_leader_proxy = LeaderProxyForPartition(par_id);
        auto proxy = pair_leader_proxy.second;
        auto future = proxy->async_AccValidate(cmd_id, ssid_new, fuattr); // call Acc Validate RPC
        Future::safe_release(future);
    }

    void AccCommo::AccBroadcastFinalize(parid_t par_id, cmdid_t cmd_id, int8_t decision) {
        rrr::FutureAttr fuattr;
        fuattr.callback = {};
        auto pair_leader_proxy = LeaderProxyForPartition(par_id);
        auto proxy = pair_leader_proxy.second;
        auto future = proxy->async_AccFinalize(cmd_id, decision, fuattr); // call AccFinalize RPC
        Future::safe_release(future);
    }

    void AccCommo::AccBroadcastFinalizeAbort(parid_t par_id, cmdid_t cmd_id, int8_t decision, const std::function<void()> &callback) {
        rrr::FutureAttr fuattr;
        fuattr.callback =
                [this, callback](Future* fu) {
                    fu->get_reply();
                    callback();
                };
        auto pair_leader_proxy = LeaderProxyForPartition(par_id);
        auto proxy = pair_leader_proxy.second;
        auto future = proxy->async_AccFinalize(cmd_id, decision, fuattr); // call AccFinalize RPC
        Future::safe_release(future);
    }

    void AccCommo::AccBroadcastStatusQuery(parid_t par_id, cmdid_t cmd_id, const std::function<void(int8_t res)> &callback) {
        rrr::FutureAttr fuattr;
        fuattr.callback =
                [this, callback](Future* fu) {
                    int8_t ret;
                    fu->get_reply() >> ret;
                    callback(ret);
                };
        auto pair_leader_proxy = LeaderProxyForPartition(par_id);
        auto proxy = pair_leader_proxy.second;
        auto future = proxy->async_AccStatusQuery(cmd_id, fuattr); // call Acc StatusQuery RPC
        Future::safe_release(future);
    }

    void AccCommo::AccBroadcastResolveStatusCoord(uint32_t coord,
                                                  uint64_t cmd_id,
                                                  const std::function<void(uint8_t status)> &callback) {
        rrr::FutureAttr fuattr;
        fuattr.callback =
                [this, callback](Future* fu) {
                    uint8_t status;
                    fu->get_reply() >> status;
                    callback(status);
                };
        auto pair_leader_proxy = LeaderProxyForPartition(coord);
        auto proxy = pair_leader_proxy.second;
        auto future = proxy->async_AccResolveStatusCoord(cmd_id, fuattr);
        Future::safe_release(future);
    }

    void AccCommo::AccBroadcastGetRecord(uint32_t cohort,
                                         uint64_t cmd_id,
                                         const function<void(uint8_t status, uint64_t ssid_low, uint64_t ssid_high)> &callback) {
        rrr::FutureAttr fuattr;
        fuattr.callback =
                [this, callback](Future* fu) {
                    uint8_t status;
                    uint64_t ssid_low;
                    uint64_t ssid_high;
                    fu->get_reply() >> status >> ssid_low >> ssid_high;
                    callback(status, ssid_low, ssid_high);
                };
        auto pair_leader_proxy = LeaderProxyForPartition(cohort);
        auto proxy = pair_leader_proxy.second;
        auto future = proxy->async_AccGetRecord(cmd_id, fuattr);
        Future::safe_release(future);
    }

    void
    AccCommo::AccBroadcastRotxnDispatch(uint32_t coo_id,
                                        shared_ptr<vector<shared_ptr<SimpleCommand>>> sp_vec_piece,
                                        Coordinator *coo,
                                        uint64_t ssid_spec,
                                        uint64_t safe_ts,
                                        const function<void(int res,
                                                            uint64_t ssid_low,
                                                            uint64_t ssid_high,
                                                            uint64_t ssid_new,
                                                            TxnOutput &,
                                                            uint64_t arrival_time,
                                                            uint8_t rotxn_okay,
                                                            const std::pair<parid_t, uint64_t>& new_svr_ts)> &callback) {
        cmdid_t cmd_id = sp_vec_piece->at(0)->root_id_;
        auto par_id = sp_vec_piece->at(0)->PartitionId();
        rrr::FutureAttr fuattr;
        fuattr.callback =
                [coo, this, callback](Future* fu) {
                    int32_t ret;
                    uint64_t ssid_low;
                    uint64_t ssid_high;
                    uint64_t ssid_new;
                    TxnOutput outputs;
                    uint64_t arrival_time;
                    uint8_t rotxn_okay;
                    std::pair<parid_t, uint64_t> new_svr_ts;
                    fu->get_reply() >> ret >> ssid_low >> ssid_high >> ssid_new >> outputs >> arrival_time >> rotxn_okay >> new_svr_ts;
                    callback(ret, ssid_low, ssid_high, ssid_new, outputs, arrival_time, rotxn_okay, new_svr_ts);
                };
        auto pair_leader_proxy = LeaderProxyForPartition(par_id);
        auto proxy = pair_leader_proxy.second;
        shared_ptr<VecPieceData> sp_vpd(new VecPieceData);
        sp_vpd->sp_vec_piece_data_ = sp_vec_piece;
        MarshallDeputy md(sp_vpd); // ????
        rrr::Future* future;
        future = proxy->async_AccRotxnDispatch(coo_id, cmd_id, md, ssid_spec, safe_ts, fuattr);
        Future::safe_release(future);

        // FIXME fix this, this cause occ and perhaps 2pl to fail
        for (auto& pair : rpc_par_proxies_[par_id]) {
            if (pair.first != pair_leader_proxy.first) {
                rrr::FutureAttr fu2, fu2_status;
                fu2.callback =
                        [coo, this, callback](Future* fu) {
                            int32_t ret;
                            uint64_t ssid_low;
                            uint64_t ssid_high;
                            uint64_t ssid_new;
                            TxnOutput outputs;
                            uint64_t arrival_time;
                            uint8_t rotxn_okay;
                            std::pair<parid_t, uint64_t> new_svr_ts;
                            fu->get_reply() >> ret >> ssid_low >> ssid_high >> ssid_new >> outputs >> arrival_time >> rotxn_okay >> new_svr_ts;
                            // do nothing
                        };
                Future::safe_release(pair.second->async_AccRotxnDispatch(coo_id, cmd_id, md, ssid_spec, safe_ts, fu2));
            }
        }
    }
}
