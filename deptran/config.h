#ifndef CONFIG_H_
#define CONFIG_H_

#include <vector>
#include <string>

namespace rococo {
class SiteInfo {
public:
  static std::map<std::string, std::string> dns_;
  static std::string name2host(std::string& site_name);
  static void        load_dns(std::string& hosts_path);

public:
  i64 id_;
  std::string name_;
  std::string host_;
  int port_;

  SiteInfo() = delete;
  SiteInfo(i64 id, std::string& site_addr) {
    id_ = id;
    auto pos = site_addr.find(':');
    verify(pos != std::string::npos);
    name_ = site_addr.substr(0, pos);
    std::string port_str = site_addr.substr(pos + 1);
    port_ = std::stoi(port_str);

    host_ = SiteInfo::name2host(name_);
  }
};

class Config {
public:

  typedef enum {
    SS_DISABLED,
    SS_THREAD_SINGLE,
    SS_PROCESS_SINGLE
  } single_server_t;

  std::map<string, int> modes_map_ = {
    { "none",          MODE_NONE                                         },
    { "2pl",           MODE_2PL                                          },
    { "occ",           MODE_OCC                                          },
    { "rcc",           MODE_RCC                                          },
    { "ro6",           MODE_RO6                                          },
    { "rpc_null",      MODE_RPC_NULL                                     },
    { "deptran",       MODE_DEPTRAN                                      },
    { "deptran_er",    MODE_DEPTRAN                                      },
    { "2pl_w",         MODE_2PL                                          },
    { "2pl_wait_die",  MODE_2PL                                          },
    { "2pl_ww",        MODE_2PL                                          },
    { "2pl_wound_die", MODE_2PL                                          }
  };

  std::map<string, mdb::symbol_t> tbl_types_map_ = {
    { "sorted",   mdb::TBL_SORTED         },
    { "unsorted", mdb::TBL_UNSORTED       },
    { "snapshot", mdb::TBL_SNAPSHOT       }
  };

private:

  static Config *config_s;
  void        load_config_xml(std::string& filename);
  void        init_hostsmap(const char *hostspath);
  std::string site2host_addr(std::string& name);
  std::string site2host_name(std::string& addr);

  // configuration for trial controller.
  // TODO replace with std::string
  char *ctrl_hostname_;
  uint32_t ctrl_port_;
  uint32_t ctrl_timeout_;
  char    *ctrl_key_;
  char    *ctrl_init_;
  uint32_t duration_;
  bool     heart_beat_;

  // common configuration
  int32_t  mode_;
  char    *logging_path_;
  int32_t  benchmark_; // workload
  uint32_t scale_factor_;
  std::vector<double> txn_weight_;

  // coordinator-side configuration
  uint32_t cid_;
  uint32_t concurrent_txn_;
  bool     batch_start_;
  bool     retry_wait_;
  uint32_t max_retry_;
  int32_t  server_or_client_; // 0 for server, 1 for client, init -1
  bool     early_return_;

  // server-side configuration
  uint32_t  sid_;
  char    **site_;
  uint32_t *site_threads_;
  uint32_t  num_site_;
  uint32_t  num_coordinator_threads_;
  uint32_t  start_coordinator_id_;
  single_server_t single_server_;

public:
  std::map<string, string> hostsmap_;

protected:

  Config();

  Config(uint32_t        cid,
         uint32_t        sid,
         std::string   & filename,
         char           *ctrl_hostname,
         uint32_t        ctrl_port,
         uint32_t        ctrl_timeout,
         char           *ctrl_key,
         char           *ctrl_init

         /*, char *ctrl_run*/,
         uint32_t        duration,
         bool            heart_beat,
         single_server_t single_server,
         int             server_or_client,
         char           *logging_path,
         char           *hostspath
         );

public:
  static int           create_config(int   argc,
                                     char *argv[]);
  static Config      * get_config();
  static void          destroy_config();

  void                 load_config_yml(std::string&);
  void                 init_mode(std::string&);
  void                 init_bench(std::string&);
  uint32_t             get_site_id();
  uint32_t             get_client_id();
  uint32_t             get_ctrl_port();
  uint32_t             get_ctrl_timeout();
  const char         * get_ctrl_hostname();
  const char         * get_ctrl_key();
  const char         * get_ctrl_init();

  // const char *get_ctrl_run();
  uint32_t             get_duration();
  bool                 do_heart_beat();
  int32_t              get_all_site_addr(std::vector<std::string>& servers);
  int32_t              get_site_addr(uint32_t     sid,
                                     std::string& server);
  int32_t              get_my_addr(std::string& server);
  int32_t              get_threads(uint32_t& threads);
  int32_t              get_mode();
  uint32_t             get_num_threads();
  uint32_t             get_start_coordinator_id();
  int32_t              get_benchmark();
  uint32_t             get_num_site();
  uint32_t             get_scale_factor();
  uint32_t             get_max_retry();
  single_server_t      get_single_server();
  uint32_t             get_concurrent_txn();
  bool                 get_batch_start();
  bool                 do_early_return();

#ifdef CPU_PROFILE
  int                  get_prof_filename(char *prof_file);
#endif // ifdef CPU_PROFILE

  bool                 do_logging();

  char               * log_path();

  bool                 retry_wait();

  std::vector<double>& get_txn_weight();

  ~Config();
};
}

#endif // ifndef CONFIG_H_
