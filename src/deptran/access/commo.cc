#include "commo.h"

namespace janus {
    void AccCommo::AccBroadcastDispatch(shared_ptr<vector<shared_ptr<SimpleCommand>>> sp_vec_piece,
                                        Coordinator *coo,
                                        snapshotid_t ssid_spec,
                                        bool single_shard,
                                        bool write_only,
                                        const std::function<void(int res,
                                                                 uint64_t ssid_low,
                                                                 uint64_t ssid_high,
                                                                 uint64_t ssid_new,
                                                                 TxnOutput &,
                                                                 uint64_t arrival_time)> &callback,
					                    cmdid_t status_cmd_id,
                                        int& n_status_query,
                                        const std::function<void(int8_t res)> &callback_status) {
        cmdid_t cmd_id = sp_vec_piece->at(0)->root_id_;
        verify(sp_vec_piece->size() > 0);
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
                    fu->get_reply() >> ret >> ssid_low >> ssid_high >> ssid_new >> outputs >> arrival_time;
                    callback(ret, ssid_low, ssid_high, ssid_new, outputs, arrival_time);
                };
        auto pair_leader_proxy = LeaderProxyForPartition(par_id);
        Log_debug("send dispatch to site %ld",
                  pair_leader_proxy.first);
        auto proxy = pair_leader_proxy.second;
        shared_ptr<VecPieceData> sp_vpd(new VecPieceData);
        sp_vpd->sp_vec_piece_data_ = sp_vec_piece;
        MarshallDeputy md(sp_vpd); // ????
        auto future = proxy->async_AccDispatch(cmd_id, md, ssid_spec, (uint8_t)single_shard, (uint8_t)write_only, fuattr); // call Acc dispatch RPC

	    // now insert AccStatusQuery RPC here
        rrr::FutureAttr status_fuattr;
        status_fuattr.callback =
                [coo, this, callback_status](Future* fu) {
                    int8_t ret;
                    fu->get_reply() >> ret;
                    callback_status(ret);
                };
        n_status_query++;
        auto future_status = proxy->async_AccStatusQuery(status_cmd_id, status_fuattr); // call Acc StatusQuery RPC

        // release both RPCs sequentially
        Future::safe_release(future);
	    Future::safe_release(future_status);

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
                            fu->get_reply() >> ret >> ssid_low >> ssid_high >> ssid_new >> outputs >> arrival_time;
                            // do nothing
                        };
		        fu2_status.callback =
                        [coo, this, callback_status](Future* fu) {
                            int8_t ret;
                            fu->get_reply() >> ret;
                            // do nothing
                        };
                Future::safe_release(pair.second->async_AccDispatch(cmd_id, md, ssid_spec, (uint8_t)single_shard, (uint8_t)write_only, fu2));
		        Future::safe_release(pair.second->async_AccStatusQuery(status_cmd_id, fu2_status));
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
}
