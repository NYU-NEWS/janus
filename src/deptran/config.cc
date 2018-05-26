//#include "all.h"
#include <algorithm>

#include "__dep__.h"
#include "multi_value.h"
#include "constants.h"
#include "config.h"
#include "multi_value.h"
#include "sharding.h"
#include "frame.h"
#include "sharding.h"
#include "rococo/dep_graph.h"
#include "rococo/graph_marshaler.h"
#include "workload.h"

// for tpca benchmark
#include "bench/tpca/workload.h"
#include "bench/tpca/payment.h"

// tpcc benchmark
#include "bench/tpcc/workload.h"
#include "bench/tpcc/procedure.h"

// tpcc dist partition benchmark
#include "bench/tpcc_dist/procedure.h"

// tpcc real dist partition benchmark
#include "bench/tpcc_real_dist/workload.h"
#include "bench/tpcc_real_dist/procedure.h"

// rw benchmark
#include "bench/rw/workload.h"
#include "bench/rw/procedure.h"

// micro bench
#include "bench/micro/workload.h"
#include "bench/micro/procedure.h"


namespace janus {
Config *Config::config_s = nullptr;


Config * Config::GetConfig() {
  verify(config_s != nullptr);
  return config_s;
}

int Config::CreateConfig(int argc, char **argv) {
  if (config_s != NULL) return -1;

//  std::string filename = "./config/sample.yml";
  vector<string> config_paths;
  std::string proc_name = "localhost"; // default as "localhost"
  std::string logging_path = "./disk_log/";
  char *end_ptr    = NULL;

  char *hostspath               = NULL;
  char *ctrl_hostname           = NULL;
  char *ctrl_key                = NULL;
  char *ctrl_init               = NULL /*, *ctrl_run = NULL*/;
  uint32_t ctrl_port        = 0;
  uint32_t ctrl_timeout     = 0;
  uint32_t duration         = 10;
  bool heart_beat               = false;
  single_server_t single_server = SS_DISABLED;
  int server_or_client          = -1;

  int c;
  optind = 1;
  string filename;
  while ((c = getopt(argc, argv, "bc:d:f:h:i:k:p:P:r:s:S:t:H:")) != -1) {
    switch (c) {
      case 'b': // heartbeat to controller
        heart_beat = true;
        break;
      case 'd': // duration
        duration = strtoul(optarg, &end_ptr, 10);

        if ((end_ptr == NULL) || (*end_ptr != '\0'))
          return -4;
        break;
      case 'P':
        proc_name = std::string(optarg);
        break;
      case 'f': // properties.xml
        filename = std::string(optarg);
        config_paths.push_back(filename);
        break;
      case 't':
        ctrl_timeout = strtoul(optarg, &end_ptr, 10);
        if ((end_ptr == NULL) || (*end_ptr != '\0')) return -4;
        break;
      case 'c': // client id
        // TODO remove
        if ((end_ptr == NULL) || (*end_ptr != '\0'))
          return -4;
        if (server_or_client != -1)
          return -4;
        server_or_client = 1;
        break;
      case 'h': // ctrl_hostname
        // TODO remove
        ctrl_hostname = (char *)malloc((strlen(optarg) + 1) * sizeof(char));
        strcpy(ctrl_hostname, optarg);
        break;
      case 'H': // ctrl_host
        // TODO remove
        hostspath = (char *)malloc((strlen(optarg) + 1) * sizeof(char));
        strcpy(hostspath, optarg);
        break;
      case 'i': // ctrl_init
        // TODO remove
        ctrl_init = (char *)malloc((strlen(optarg) + 1) * sizeof(char));
        strcpy(ctrl_init, optarg);
        break;
      case 'k':
        // TODO remove
        ctrl_key = (char *)malloc((strlen(optarg) + 1) * sizeof(char));
        strcpy(ctrl_key, optarg);
        break;
      case 'r': // logging path
        // TODO remove
        logging_path = string(optarg);
        break;
      case 'p':
        // TODO remove
        ctrl_port = strtoul(optarg, &end_ptr, 10);
        if ((end_ptr == NULL) || (*end_ptr != '\0')) return -4;
        break;
      case 's': // site id
        // TODO remove
        if ((end_ptr == NULL) || (*end_ptr != '\0')) return -4;

        if (server_or_client != -1) return -4;
        server_or_client = 0;
        break;
      case 'S': // client touch only single server
      {
        // TODO remove
        int single_server_buf = strtoul(optarg, &end_ptr, 10);

        if ((end_ptr == NULL) || (*end_ptr != '\0')) return -4;

        switch (single_server_buf) {
          case 0:
            single_server = SS_DISABLED;
            break;
          case 1:
            single_server = SS_THREAD_SINGLE;
            break;
          case 2:
            single_server = SS_PROCESS_SINGLE;
            break;
          default:
            return -4;
        }
        break;
      }
      case '?':
        // TODO remove
        if ((optopt == 'c') ||
            (optopt == 'd') ||
            (optopt == 'f') ||
            (optopt == 'h') ||
            (optopt == 'i') ||
            (optopt == 'k') ||
            (optopt == 'p') ||
            (optopt == 'r') ||
            (optopt == 's') ||
            (optopt == 't')) Log_error("Option -%c requires an argument.",
                                       optopt);
        else if (isprint(optopt)) Log_error("Unknown option -%c.", optopt);
        else Log_error("Unknown option \\x%x", optopt);
        return -2;
      default:
        return -3;
    }
  }

//  if ((server_or_client != 0) && (server_or_client != 1)) return -5;
  verify(config_s == nullptr);
  config_s = new Config(
    ctrl_hostname,
    ctrl_port,
    ctrl_timeout,
    ctrl_key,
    ctrl_init,

    // ctrl_run,
    duration,
    heart_beat,
    single_server,
    logging_path);
  config_s->proc_name_ = proc_name;
  config_s->config_paths_ = config_paths;
  config_s->Load();
  return SUCCESS;
}

void Config::DestroyConfig() {
  if (config_s) {
    delete config_s;
    config_s = NULL;
  }
}

//Config::Config() {}

Config::Config(char           *ctrl_hostname,
               uint32_t        ctrl_port,
               uint32_t        ctrl_timeout,
               char           *ctrl_key,
               char           *ctrl_init,
               uint32_t        duration,
               bool            heart_beat,
               single_server_t single_server,
               string           logging_path) :
  heart_beat_(heart_beat),
  ctrl_hostname_(ctrl_hostname),
  ctrl_port_(ctrl_port),
  ctrl_timeout_(ctrl_timeout),
  ctrl_key_(ctrl_key),
  ctrl_init_(ctrl_init),
  duration_(duration),
  config_paths_(vector<string>()),
  tx_proto_(0),
  proc_id_(0),
  benchmark_(0),
  scale_factor_(1),
  txn_weight_(vector<double>()),
  txn_weights_(map<string, double>()),
  proc_name_(string()),
  batch_start_(false),
  early_return_(false),
  retry_wait_(false),
  logging_path_(logging_path),
  single_server_(single_server),
  n_concurrent_(1),
  max_retry_(1),
  num_site_(0),
  start_coordinator_id_(0),
  site_(vector<string>()),
  site_threads_(vector<uint32_t>()),
  num_coordinator_threads_(1),
  sid_(1),
  cid_(1),
  next_site_id_(0),
  proc_host_map_(map<string, string>()),
  sharding_(nullptr)
{
}

void Config::Load() {
  for (auto &name: config_paths_) {
    if (boost::algorithm::ends_with(name, "yml")) {
      LoadYML(name);
    } else {
      verify(0);
    }
  }

  // TODO particular configuration for certain workloads.
  this->InitTPCCD();
}

void Config::LoadYML(std::string &filename) {
  Log_info("%s: %s", __FUNCTION__, filename.c_str());
  YAML::Node config = YAML::LoadFile(filename);

  if (config["process"]) {
    BuildSiteProcMap(config["process"]);
  }
  if (config["site"]) {
    LoadSiteYML(config["site"]);
  }
  if (config["process"]) {
    LoadProcYML(config["process"]);
  }
  if (config["host"]) {
    LoadHostYML(config["host"]);
  }
  if (config["mode"]) {
    LoadModeYML(config["mode"]);
  }
  if (config["bench"]) {
    LoadBenchYML(config["bench"]);
  }
  if (config["schema"]) {
    LoadSchemaYML(config["schema"]);
  }
  if (config["sharding"]) {
    LoadShardingYML(config["sharding"]);
  }
  if (config["client"]) {
    LoadClientYML(config["client"]);
  }
  if (config["n_concurrent"]) {
    n_concurrent_ = config["n_concurrent"].as<uint16_t>();
    Log_info("# of concurrent requests: %d", n_concurrent_);
  }
  if (config["n_parallel_dispatch"]) {
    n_parallel_dispatch_ = config["n_parallel_dispatch"].as<int32_t>();
  }
}

void Config::LoadSiteYML(YAML::Node config) {
  auto servers = config["server"];
  int partition_id = 0;
  int site_id = 0; // start from
  int locale_id = 0;

  // count the sites so that we can reserve storage up front
  // to avoid invalidating the pointers
  int num_sites=0;
  for (auto partition = servers.begin(); partition != servers.end(); partition++) {
    num_sites += partition->size();
  }
  sites_.reserve(num_sites);

  for (auto server_it = servers.begin(); server_it != servers.end(); server_it++) {
    auto group = *server_it;
    locale_id=0;
    ReplicaGroup replica_group(partition_id);
    for (auto group_it = group.begin(); group_it != group.end(); group_it++) {
      auto site_addr = group_it->as<string>();
      SiteInfo info(site_id++, site_addr);
      info.partition_id_ = replica_group.partition_id;
      info.locale_id = locale_id;
      info.type_ = SERVER;
      info.proc_name = site_proc_map_[info.name];
      sites_.push_back(info);
      replica_group.replicas.push_back(&sites_.back());
      locale_id++;
    }
    replica_groups_.push_back(replica_group);
    partition_id++;
  }

  auto clients = config["client"];
  for (auto client_it = clients.begin(); client_it != clients.end(); client_it++) {
    auto group = *client_it;
    int locale_id = 0;
    for (auto group_it = group.begin(); group_it != group.end(); group_it++) {
      auto site_name = group_it->as<string>();
      SiteInfo info(site_id++);
      info.name = site_name;
      info.proc_name = site_proc_map_[info.name];
      info.type_ = CLIENT;
      info.locale_id = locale_id;
      info.port = GetClientPort(site_name);
      par_clients_.push_back(info);
      locale_id++;
    }
  }
}

// TODO: inefficient -- do not call during testing
// port assignment should match run.py get_process_info function
int Config::GetClientPort(std::string site_name) {
  auto config = Config::GetConfig();
  std::vector<std::string> sites;
  for (auto site_host_pair : site_proc_map_) {
    sites.push_back(site_host_pair.first);
  }
  verify(sites.size() > 0);
  std::sort(sites.begin(), sites.end());
  std::vector<std::string> hosts;
  for (auto s : sites) {
    if (std::find(hosts.begin(), hosts.end(), site_proc_map_[s]) == hosts.end()) {
      if (s == site_name) {
        return Config::BASE_CLIENT_CTRL_PORT + hosts.size();
      } else {
        hosts.push_back(site_proc_map_[s]);
      }
    }
  }
//  verify(0);
  return -1;
}

void Config::BuildSiteProcMap(YAML::Node process) {
  Log_info("%s", __FUNCTION__);
  for (auto it = process.begin(); it != process.end(); it++) {
    auto site_name = it->first.as<string>();
    auto proc_name = it->second.as<string>();

    site_proc_map_[site_name] = proc_name;
  }
}

void Config::LoadProcYML(YAML::Node config) {
  for (auto it = config.begin(); it != config.end(); it++) {
    auto site_name = it->first.as<string>();
    auto proc_name = it->second.as<string>();
    auto info = SiteByName(site_name);
//    verify(info != nullptr);
    if (info != nullptr && proc_name != "") {
      info->proc_name = proc_name;
    }
  }
}

void Config::LoadHostYML(YAML::Node config) {
  for (auto it = config.begin(); it != config.end(); it++) {
    auto proc_name = it->first.as<string>();
    auto host_name = it->second.as<string>();
    proc_host_map_[proc_name] = host_name;
    for (auto& group : replica_groups_) {
      for (auto& server : group.replicas) {
        if (server->proc_name == proc_name) {
          server->host = host_name;
        }
      }
    }
    for (auto& client : par_clients_) {
        if (client.proc_name == proc_name) {
          client.host = host_name;
        }
    }
  }
}

void Config::InitMode(string &cc_name, string& ab_name) {
  tx_proto_ = Frame::Name2Mode(cc_name);

  if ((cc_name == "rococo") || (cc_name == "deptran")) {
    // deprecated
    early_return_ = false;
  } else if (cc_name == "deptran_er") {
    // deprecated
    early_return_ = true;
  } else if (cc_name == "2pl_w") {
    retry_wait_ = true;
  } else if (cc_name == "2pl_wait_die" || cc_name == "2pl_wd") {
    mdb::FineLockedRow::set_wait_die();
  } else if ((cc_name == "2pl_ww") || (cc_name == "2pl_wound_die")) {
    mdb::FineLockedRow::set_wound_wait();
    n_parallel_dispatch_ = 1;
  }

  replica_proto_ = Frame::Name2Mode(ab_name);
}

void Config::InitBench(std::string &bench_str) {
  if (bench_str == "tpca") {
    benchmark_ = TPCA;
  } else if (bench_str == "tpcc") {
    benchmark_ = TPCC;
    n_parallel_dispatch_ = 0;
  } else if (bench_str == "tpcc_dist_part") {
    benchmark_ = TPCC_DIST_PART;
  } else if (bench_str == "tpccd") {
    benchmark_ = TPCC_REAL_DIST_PART;
  } else if (bench_str == "tpcc_real_dist_part") {
    benchmark_ = TPCC_REAL_DIST_PART;
  } else if (bench_str == "rw") {
    benchmark_ = RW_BENCHMARK;
  } else if (bench_str == "micro_bench") {
    benchmark_ = MICRO_BENCH;
  } else {
    Log_error("No implementation for benchmark: %s", bench_str.c_str());
    verify(0);
  }
}

std::string Config::site2host_addr(std::string& siteaddr) {
  auto pos = siteaddr.find_first_of(':');

  verify(pos != std::string::npos);
  std::string sitename = siteaddr.substr(0, pos);
  std::string hostname = site2host_name(sitename);
  std::string hostaddr = siteaddr.replace(0, pos, hostname);
  return hostaddr;
}

std::string Config::site2host_name(std::string& sitename) {
  //    Log::debug("find host name by site name: %s", sitename.c_str());
  auto it = proc_host_map_.find(sitename);

  if (it != proc_host_map_.end()) {
    return it->second;
  } else {
    return sitename;
  }
}


void Config::LoadModeYML(YAML::Node config) {
  auto mode_str = config["cc"].as<string>();
  boost::algorithm::to_lower(mode_str);
  auto ab_str = config["ab"].as<string>();
  boost::algorithm::to_lower(ab_str);
  this->InitMode(mode_str, ab_str);
  max_retry_ = config["retry"].as<int32_t>();
//  concurrent_txn_ = config["ongoing"].as<uint32_t>();
  batch_start_ = config["batch"].as<bool>();
  if (config["timestamp"]) {
    string ts_str = config["timestamp"].as<string>();
    boost::algorithm::to_lower(ts_str);
    if (ts_str == "clock") {
      timestamp_ = CLOCK;
    } else if (ts_str == "counter") {
      timestamp_ = COUNTER;
    } else {
      verify(0);
    }
  }
}

void Config::LoadBenchYML(YAML::Node config) {
  std::string bench_str = config["workload"].as<string>();
  this->InitBench(bench_str);
  scale_factor_ = config["scale"].as<uint32_t>();
  auto weights = config["weight"];
  for (auto it = weights.begin(); it != weights.end(); it++) {
    auto txn_name = it->first.as<string>();
    auto weight = it->second.as<double>();
    txn_weights_[txn_name] = weight;
  }

  txn_weight_.push_back(txn_weights_["new_order"]);
  txn_weight_.push_back(txn_weights_["payment"]);
  txn_weight_.push_back(txn_weights_["order_status"]);
  txn_weight_.push_back(txn_weights_["delivery"]);
  txn_weight_.push_back(txn_weights_["stock_level"]);

  sharding_ = Frame(MODE_NONE).CreateSharding();
  auto populations = config["population"];
  auto &tb_infos = sharding_->tb_infos_;
  for (auto it = populations.begin(); it != populations.end(); it++) {
    auto tbl_name = it->first.as<string>();
    auto info_it = tb_infos.find(tbl_name);
    if(info_it == tb_infos.end()) {
      tb_infos[tbl_name] = Sharding::tb_info_t();
      info_it = tb_infos.find(tbl_name);
    }
    auto &tbl_info = info_it->second;
    int pop = it->second.as<int>();
    tbl_info.num_records = scale_factor_ * pop;
    verify(tbl_info.num_records > 0);
  }
  if (config["dist"])
    dist_ = config["dist"].as<string>();
  if (config["coefficient"])
    coeffcient_ = config["coefficient"].as<float>();
  if (config["rotate"])
    rotate_ = config["rotate"].as<int32_t>();
}

void Config::LoadSchemaYML(YAML::Node config) {
  verify(sharding_);
  auto &tb_infos = sharding_->tb_infos_;
  for (auto it = config.begin(); it != config.end(); it++) {
    auto table_node = *it;
    std::string tbl_name = table_node["name"].as<string>();

    auto info_it = tb_infos.find(tbl_name);
    if(info_it == tb_infos.end()) {
      tb_infos[tbl_name] = Sharding::tb_info_t();
      info_it = tb_infos.find(tbl_name);
    }
    auto &tbl_info = info_it->second;
    auto columns = table_node["column"];
    for (auto iitt = columns.begin(); iitt != columns.end(); iitt++) {
      auto column = *iitt;
      LoadSchemaTableColumnYML(tbl_info, column);
    }

    tbl_info.tb_name = tbl_name;
    sharding_->tb_infos_[tbl_name] = tbl_info;
  }
  sharding_->BuildTableInfoPtr();
}

void Config::LoadSchemaTableColumnYML(Sharding::tb_info_t &tb_info,
                                      YAML::Node column) {
  std::string c_type = column["type"].as<string>();
  verify(c_type.size() > 0);
  Value::kind c_v_type;

  if (c_type == "i32" || c_type == "integer") {
    c_v_type = Value::I32;
  } else if (c_type == "i64") {
    c_v_type = Value::I64;
  } else if (c_type == "double") {
    c_v_type = Value::DOUBLE;
  } else if (c_type == "str" || c_type == "string") {
    c_v_type = Value::STR;
  } else {
    c_v_type = Value::UNKNOWN;
    verify(0);
  }

  std::string c_name = column["name"].as<string>();
  verify(c_name.size() > 0);

  bool c_primary = column["primary"].as<bool>(false);

  std::string c_foreign = column["foreign"].as<string>("");
  Sharding::column_t  *foreign_key_column = NULL;
  Sharding::tb_info_t *foreign_key_tb     = NULL;
  std::string ftbl_name;
  std::string fcol_name;
  bool is_foreign = (c_foreign.size() > 0);
  if (is_foreign) {
    size_t pos = c_foreign.find('.');
    verify(pos != std::string::npos);

    ftbl_name = c_foreign.substr(0, pos);
    fcol_name = c_foreign.substr(pos + 1);
    verify(c_foreign.size() > pos + 1);
  }
  tb_info.columns.push_back(Sharding::column_t(c_v_type,
                                               c_name,
                                               c_primary,
                                               is_foreign,
                                               ftbl_name,
                                               fcol_name));
}

void Config::LoadShardingYML(YAML::Node config) {
  verify(sharding_);
  auto &tb_infos = sharding_->tb_infos_;
  for (auto it = config.begin(); it != config.end(); it++) {
    auto tbl_name = it->first.as<string>();
    auto info_it = tb_infos.find(tbl_name);
    verify(info_it != tb_infos.end());
    auto &tbl_info = info_it->second;
    string method = it->second.as<string>();

    Log_info("group size: %d", replica_groups_.size());
    for (auto replica_group_it = this->replica_groups_.begin();
         replica_group_it != this->replica_groups_.end();
         replica_group_it++) {
      auto &replica_group = *replica_group_it;
      tbl_info.par_ids.push_back(replica_group.partition_id);
      tbl_info.symbol = tbl_types_map_["sorted"];
    }
  
    verify(tbl_info.par_ids.size() > 0);
  }
}

void Config::LoadClientYML(YAML::Node client) {
  std::string type = client["type"].as<std::string>();
  std::transform(type.begin(), type.end(), type.begin(), ::tolower);
  if (type == "open") {
    client_type_ = Open;
    client_rate_ = client["rate"].as<int>();
  } else {
    client_type_ = Closed;
    client_rate_ = -1;
  }
  forwarding_enabled_ = client["forwarding"].as<bool>(false);
  Log_info("client forwarding: %d", forwarding_enabled_);
}

void Config::InitTPCCD() {
  // TODO particular configuration for certain workloads.
  auto &tb_infos = sharding_->tb_infos_;
  if (benchmark_ == TPCC_REAL_DIST_PART) {
    i32 n_w_id =
        (i32)(tb_infos[std::string(TPCC_TB_WAREHOUSE)].num_records);
    verify(n_w_id > 0);
    i32 n_d_id = (i32)(GetNumPartition() *
        tb_infos[std::string(TPCC_TB_DISTRICT)].num_records / n_w_id);
    i32 d_id = 0, w_id = 0;

    for (d_id = 0; d_id < n_d_id; d_id++)
      for (w_id = 0; w_id < n_w_id; w_id++) {
        MultiValue mv(std::vector<Value>({Value(d_id),
                                          Value(w_id)}));
        sharding_->insert_dist_mapping(mv);
      }
    i32 n_i_id =
        (i32)(tb_infos[std::string(TPCC_TB_ITEM)].num_records);
    i32 i_id = 0;

    for (i_id = 0; i_id < n_i_id; i_id++)
      for (w_id = 0; w_id < n_w_id; w_id++) {
        MultiValue mv(std::vector<Value>({
                                             Value(i_id),
                                             Value(w_id)
                                         }));
        sharding_->insert_stock_mapping(mv);
      }
  }
}

Config::~Config() {
  if (sharding_) {
    delete sharding_;
    sharding_ = NULL;
  }

  if (ctrl_hostname_) {
    free(ctrl_hostname_);
    ctrl_hostname_ = NULL;
  }

  if (ctrl_key_) {
    free(ctrl_key_);
    ctrl_key_ = NULL;
  }

  if (ctrl_init_) {
    free(ctrl_init_);
    ctrl_init_ = NULL;
  }
}

unsigned int Config::get_site_id() {
  verify(0);
  return sid_;
}

unsigned int Config::get_client_id() {
  verify(0);
  return cid_;
}

unsigned int Config::get_ctrl_port() {
  return ctrl_port_;
}

unsigned int Config::get_ctrl_timeout() {
  return ctrl_timeout_;
}

const char * Config::get_ctrl_hostname() {
  return ctrl_hostname_;
}

const char * Config::get_ctrl_key() {
  return ctrl_key_;
}

const char * Config::get_ctrl_init() {
  return ctrl_init_;
}

// TODO obsolete
int Config::get_all_site_addr(std::vector<std::string>& servers) {
    const int num_servers = this->NumSites();
    for (int i=0; i<num_servers; i++) {
      auto& site = const_cast<SiteInfo&>(SiteById(i));
      servers.push_back(site.GetHostAddr());
    }
    return num_servers;
}

int Config::get_site_addr(unsigned int sid, std::string& server) {
  auto site = SiteById(sid);
  server.assign(site.GetHostAddr());
  return 1;
}

int Config::NumSites(SiteInfoType type) {
  std::vector<SiteInfo>* searching;
  if (type == SERVER) {
    searching = &sites_;
  } else {
    searching = &par_clients_;
  }
  return searching->size();
}

const Config::SiteInfo& Config::SiteById(uint32_t id) {
  verify(id >= 0);
  Config::SiteInfo* s;
  if (id<sites_.size()) {
    s = &sites_[id];
  } else {
    verify((id-sites_.size()) < par_clients_.size());
    s = &par_clients_[id-sites_.size()];
  }
  verify(s->id==id);
  return *s;
}

std::vector<Config::SiteInfo> Config::SitesByPartitionId(
    parid_t partition_id) {
  std::vector<SiteInfo> result;
  auto it = find_if(replica_groups_.begin(), replica_groups_.end(),
                    [partition_id](const ReplicaGroup& g) {
                      return g.partition_id == partition_id;
                    });
  if (it != replica_groups_.end()) {
    for (auto si : it->replicas) {
      result.push_back(*si);
    }
    return result;
  }
  verify(0);
}

int Config::GetPartitionSize(parid_t partition_id) {
  auto it = find_if(replica_groups_.begin(), replica_groups_.end(),
                    [partition_id](const ReplicaGroup& g) {
                      return g.partition_id == partition_id;
                    });
  if (it != replica_groups_.end()) {
    return it->replicas.size();
  }
  verify(0);
}

std::vector<Config::SiteInfo>
Config::SitesByLocaleId(uint32_t locale_id, SiteInfoType type) {
  std::vector<SiteInfo> result;
  std::vector<SiteInfo>* searching;
  if (type==SERVER) {
    searching = &sites_;
  } else {
    searching = &par_clients_;
  }
  std::for_each(searching->begin(), searching->end(),
                [locale_id, type, &result](SiteInfo& site) mutable {
                  if (site.locale_id==locale_id) {
                    result.push_back(site);
                  }
                });
  return result;
}

vector<Config::SiteInfo>
Config::SitesByProcessName(string proc_name, Config::SiteInfoType type) {
  std::vector<SiteInfo> result;
  std::vector<SiteInfo>* searching;
  if (type==SERVER) {
    searching = &sites_;
  } else {
    searching = &par_clients_;
  }

  std::for_each(searching->begin(), searching->end(),
                [proc_name, &result](SiteInfo& site) mutable {
                  if (site.proc_name == "") {
                    Log_fatal("cannot find proc name for site %s", site.name.c_str());
                  }
                  if (site.proc_name==proc_name) {
                    result.push_back(site);
                  }
                });
  return result;
}

Config::SiteInfo* Config::SiteByName(std::string name) {
  for (SiteInfo& site : sites_) {
    if (site.name == name) {
      return &site;
    }
  }
  for (auto& client : par_clients_) {
    if (client.name == name) {
      return &client;
    }
  }
  return nullptr;
}

int Config::get_threads(unsigned int& threads) {
  verify(0);
  if (site_threads_.size() == 0) return -1;

  if (sid_ >= num_site_) return -2;
  threads = site_threads_[sid_];
  return 0;
}

unsigned int Config::get_duration() {
  return duration_;
}

bool Config::do_heart_beat() {
  return heart_beat_;
}

int Config::get_mode() {
  return tx_proto_;
}

unsigned int Config::get_num_threads() {
  verify(num_coordinator_threads_ > 0);
  return num_coordinator_threads_;
}

unsigned int Config::get_start_coordinator_id() {
  return start_coordinator_id_;
}

int Config::benchmark() {
  return benchmark_;
}

unsigned int Config::GetNumPartition() {
  // TODO FIXME this should be number of partition.
  return replica_groups_.size();
//  return GetMyServers().size();
}

unsigned int Config::get_scale_factor() {
  return scale_factor_;
}

int32_t Config::get_max_retry() {
  return max_retry_;
}

Config::single_server_t Config::get_single_server() {
  return single_server_;
}

unsigned int Config::get_concurrent_txn() {
  return n_concurrent_;
}

bool Config::get_batch_start() {
  return batch_start_;
}

std::vector<double>& Config::get_txn_weight() {
  return txn_weight_;
}

std::map<string, double>& Config::get_txn_weights() {
  return txn_weights_;
};

int Config::GetProfilePath(char *prof_file) {
  if (prof_file == NULL) return -1;
  return sprintf(prof_file, "process-%s.prof", proc_name_.c_str());
}

bool Config::do_early_return() {
  return early_return_;
}

bool Config::do_logging() {
  return logging_path_.empty();
}

bool Config::IsReplicated() {
  // TODO
  return (replica_proto_ != MODE_NONE);
  return true;
}

const char * Config::log_path() {
  return logging_path_.c_str();
}

bool Config::retry_wait() {
  return retry_wait_;
}



}
