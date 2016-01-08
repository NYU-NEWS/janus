//
// Created by lamont on 12/22/15.
//

#include "communicator.h"

namespace mdcc {
  void MdccCommunicator::SendStart(StartRequest& req,
                                   std::function<void(StartResponse&)>& callback) {
    const Config::SiteInfo* target_site;
    auto local_sites = config_->SitesByLocaleId(site_info_.locale_id);
    if (local_sites.size() > 0) {
      target_site = ChooseRandomSite(local_sites);
    } else {
      target_site = ChooseRandomSite(config_->sites_);
    }

    Log::info("%s txn %ld from site %d to site %d", __FUNCTION__, req.txn_id, site_info_.id, target_site->id);
    auto& proxy = site_proxies_[target_site->id];

    rrr::FutureAttr future;
    future.callback = [this, req, target_site, callback] (Future *fu) {
      StartResponse res;
      Log_debug("SendStart callback for %ld from %ld", req.txn_id, target_site->id);
      auto& marshall = fu->get_reply();
      marshall >> res;
      callback(res);
    };

    Future::safe_release(proxy->client->async_Start(req, future));
  }

  const Config::SiteInfo* MdccCommunicator::ChooseRandomSite(const vector<Config::SiteInfo>& sites) {
    return &sites[RandomGenerator::rand(0, sites.size()-1)];
  }
}
