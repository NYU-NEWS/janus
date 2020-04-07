#include <rrr/base/all.hpp>
#include "ssid_predictor.h"
#include "chrono"

namespace janus {
    TIME_MAP SSIDPredictor::server_time_tracker;
    bool SSIDPredictor::INITIALIZED = false;

    uint64_t SSIDPredictor::get_current_time() {
        auto now = std::chrono::system_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch()).count();
    }

    void SSIDPredictor::update_time_tracker(parid_t server, uint64_t time) {
        if (!INITIALIZED) {
            server_time_tracker.reserve(N_SERVERS);
        }
        server_time_tracker[server] = time;
    }

    uint64_t SSIDPredictor::predict_ssid(const std::vector<parid_t> &servers) {
        uint64_t time = 0;
        for (auto server : servers) {
            if (server_time_tracker.find(server) != server_time_tracker.end()
                && server_time_tracker.at(server) > time) {
                //Log_info("SSIDPredictor:predict_ssid. time = %lu. server = %u. servertime = %lu.", time, server, server_time_tracker.at(server));
                time = server_time_tracker.at(server);
            }
        }
        //Log_info("SSIDPredictor:predict_ssid. returned time = %lu.", time);
        return time;  // if 0, then the client will use its current clock time
    }
}