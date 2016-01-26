//
// Created by lamont on 12/22/15.
//

#include "communicator.h"

namespace mdcc {
  // initiate a mdcc transaction
  void MdccCommunicator::SendStart(StartRequest& req,
                                   std::function<void(StartResponse&)>& callback) {
    std::lock_guard<std::mutex> lock(this->mtx_);
    auto local_sites = config_->SitesByLocaleId(site_info_.locale_id);
    auto proxy = (local_sites.size()>0) ? RandomSite(local_sites) : RandomSite(config_->sites_);

    Log::info("%s txn %ld from site %d to site %d", __FUNCTION__, req.txn_id, site_info_.id, proxy->site_info.id);

    rrr::FutureAttr future;
    future.callback = [this, req, proxy, callback] (Future *fu) {
      StartResponse res;
      Log_debug("SendStart callback for %ld from %ld", req.txn_id, proxy->site_info.id);
      auto& marshall = fu->get_reply();
      marshall >> res;
      callback(res);
    };

    Future::safe_release(proxy->client->async_Start(req, future));
  }

  // start a transaction piece on a partition
  void MdccCommunicator::SendStartPiece(const rococo::SimpleCommand& cmd,
                      std::function<void(StartPieceResponse&)>& callback) {
    std::lock_guard<std::mutex> lock(this->mtx_);
    auto& site = config_->SiteById(cmd.GetSiteId());
    auto proxy = ClosestSiteInPartition(site.partition_id_);

    rrr::FutureAttr future;
    future.callback = [cmd, callback, proxy] (Future *fu) {
      StartPieceResponse res;
      Log_debug("SendStartPiece callback for %ld from %ld", cmd.id_, proxy->site_info.id);
      auto& marshall = fu->get_reply();
      marshall >> res;
      callback(res);
    };

    Future::safe_release(proxy->client->async_StartPiece(cmd, future));
  }

  MdccCommunicator::SiteProxy* MdccCommunicator::ClosestSiteInPartition(uint32_t partition_id) const {
    auto& partition_sites = config_->SitesByPartitionId(partition_id);
    assert(partition_sites.size()>0);
    auto it = find_if(partition_sites.begin(), partition_sites.end(),
                      [this](const Config::SiteInfo& si) {return si.locale_id==this->site_info_.locale_id;});
    verify(it != partition_sites.end());
    auto site_id = (*it).id;
    assert(site_id >= 0 && site_id < site_proxies_.size());
    return site_proxies_[site_id];
  }

  MdccCommunicator::SiteProxy* MdccCommunicator::RandomSite(const vector<Config::SiteInfo> &sites) {
    return site_proxies_[sites[RandomGenerator::rand(0, sites.size()-1)].id];
  }
}
