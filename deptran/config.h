#ifndef CONFIG_H_
#define CONFIG_H_

#include "__dep__.h"
#include "constants.h"
#include "sharding.h"

namespace rococo {

class Config {
 public:

  typedef enum {
    SS_DISABLED,
    SS_THREAD_SINGLE,
    SS_PROCESS_SINGLE
  } single_server_t;

  std::map<string, int> modes_map_ = {
      {"none", MODE_NONE},
      {"2pl", MODE_2PL},
      {"occ", MODE_OCC},
      {"rcc", MODE_RCC},
      {"ro6", MODE_RO6},
      {"brq", MODE_BRQ},
      {"rpc_null", MODE_RPC_NULL},
      {"deptran", MODE_DEPTRAN},
      {"deptran_er", MODE_DEPTRAN},
      {"2pl_w", MODE_2PL},
      {"2pl_wait_die", MODE_2PL},
      {"2pl_ww", MODE_2PL},
      {"2pl_wound_die", MODE_2PL},
      {"mdcc", MODE_MDCC}
  };

  std::map<string, mdb::symbol_t> tbl_types_map_ = {
      {"sorted", mdb::TBL_SORTED},
      {"unsorted", mdb::TBL_UNSORTED},
      {"snapshot", mdb::TBL_SNAPSHOT}
  };

 public:

  static Config *config_s;
  void init_hostsmap(const char *hostspath);
  std::string site2host_addr(std::string &name);
  std::string site2host_name(std::string &addr);

  bool heart_beat_;
  // configuration for trial controller.
  char *ctrl_hostname_;
  uint32_t ctrl_port_;
  uint32_t ctrl_timeout_;
  char *ctrl_key_;
  char *ctrl_init_;
  uint32_t duration_;
  vector<string> config_paths_;

  // common configuration

  int32_t mode_;
  uint32_t proc_id_;
  int32_t benchmark_; // workload
  uint32_t scale_factor_ = 1; // currently disabled
  std::vector<double> txn_weight_;
  map<string, double> txn_weights_;
  std::string proc_name_;
  bool batch_start_;
  bool early_return_;
  bool retry_wait_;
  string logging_path_;
  single_server_t single_server_;
  uint32_t concurrent_txn_;
  uint32_t max_retry_;

  // TODO remove, will cause problems.
  uint32_t num_site_;
  uint32_t start_coordinator_id_;
  vector<string> site_;
  vector<uint32_t> site_threads_;
  uint32_t num_coordinator_threads_;
  uint32_t sid_;
  uint32_t cid_;
  
  enum SiteInfoType { CLIENT, SERVER };
  struct SiteInfo {
    uint32_t id;
    string name;
    string addr;
    string proc;
    string host;
    uint32_t port;
    uint32_t n_thread;   // should be 1 for now
    SiteInfoType type_; 
    string proc_name;

    SiteInfo() = delete;
    SiteInfo(uint32_t id) {
      this->id = id;
    }
    SiteInfo(uint32_t id, std::string &site_addr) {
      this->id = id;
      auto pos = site_addr.find(':');
      verify(pos != std::string::npos);
      name = site_addr.substr(0, pos);
      std::string port_str = site_addr.substr(pos + 1);
      port = std::stoi(port_str);
    }

    string GetBindAddress() {
      string ret("0.0.0.0:");
      ret = ret.append(std::to_string(port));
      return ret;
    }

    string GetHostAddr() {
      if (proc.empty())
        proc = name;
      if (host.empty())
        host = proc;
      string ret;
      ret = ret.append(host).append(":").append(std::to_string(port));
      return ret;
    }
  };

  struct ReplicaGroup {
    parid_t partition_id;
    std::vector<SiteInfo> replicas;
    ReplicaGroup(parid_t id) : partition_id(id) {}
  };

  uint32_t next_site_id_;
  vector<ReplicaGroup> replica_groups_;
  vector<SiteInfo> par_clients_;
  map<string, string> proc_host_map_;

  Sharding* sharding_;

 protected:

  Config() = default;

  Config(char *ctrl_hostname,
         uint32_t ctrl_port,
         uint32_t ctrl_timeout,
         char *ctrl_key,
         char *ctrl_init,
         uint32_t duration,
         bool heart_beat,
         single_server_t single_server,
         string logging_path
  );

 public:
  static int CreateConfig(int argc,
                          char **argv);
  static Config *GetConfig();
  static void DestroyConfig();

  void InitTPCCD();

  void Load();

  void LoadYML(std::string &);
  void LoadSiteYML(YAML::Node config);
  void LoadProcYML(YAML::Node config);
  void LoadHostYML(YAML::Node config);
  void LoadModeYML(YAML::Node config);
  void LoadBenchYML(YAML::Node config);
  void LoadShardingYML(YAML::Node config);
  void LoadSchemaYML(YAML::Node config);
  void LoadSchemaTableColumnYML(Sharding::tb_info_t &tb_info,
                                YAML::Node column);
//
//  void LoadModeYML();
//  void LoadSchemeYML();
//  void LoadWorkloadYML();

  vector<SiteInfo> GetMyServers() {
    vector<SiteInfo> ret;
    for (auto& group : replica_groups_) {
      auto& replicas = group.replicas;
      ret.insert(ret.end(), replicas.begin(), replicas.end());
    }
    return ret;
  }

  vector<SiteInfo> GetMyClients() {
    vector<SiteInfo> ret;
    ret.insert(ret.end(), par_clients_.begin(), par_clients_.end());
    return ret;
  }

  SiteInfo* SiteByName(std::string name) {
    for (auto& group : replica_groups_) {
      auto& replicas = group.replicas;
      for (SiteInfo& replica : replicas) {
        if (replica.name == name) {
          return &replica;
        }
      }
    }
    for (auto& client : par_clients_) {
      if (client.name == name) {
        return &client;
      }
    }
    return nullptr;
  }

  void init_mode(std::string &);
  void init_bench(std::string &);

  uint32_t get_site_id();
  uint32_t get_client_id();
  uint32_t get_ctrl_port();
  uint32_t get_ctrl_timeout();
  const char *get_ctrl_hostname();
  const char *get_ctrl_key();
  const char *get_ctrl_init();

  // const char *get_ctrl_run();
  uint32_t get_duration();
  bool do_heart_beat();
  int32_t get_all_site_addr(std::vector<std::string> &servers);
  int32_t get_site_addr(uint32_t sid,
                        std::string &server);
//  int32_t get_my_addr(std::string &server);
  int32_t get_threads(uint32_t &threads);
  int32_t get_mode();
  uint32_t get_num_threads();
  uint32_t get_start_coordinator_id();
  int32_t get_benchmark();
  uint32_t get_num_site();
  uint32_t get_scale_factor();
  uint32_t get_max_retry();
  single_server_t get_single_server();
  uint32_t get_concurrent_txn();
  bool get_batch_start();
  bool do_early_return();

#ifdef CPU_PROFILE
  int                  get_prof_filename(char *prof_file);
#endif // ifdef CPU_PROFILE

  bool do_logging();

  const char *log_path();

  bool retry_wait();

  std::vector<double> &get_txn_weight();

  ~Config();

  };
}

#endif // ifndef CONFIG_H_
