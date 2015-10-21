//#include "all.h"

#include "__dep__.h"
#include "multi_value.h"
#include "constants.h"
#include "config.h"
#include "multi_value.h"
#include "sharding.h"
#include "piece.h"

// for tpca benchmark
#include "tpca/piece.h"
#include "tpca/chopper.h"

// tpcc benchmark
#include "tpcc/piece.h"
#include "tpcc/chopper.h"

// tpcc dist partition benchmark
#include "tpcc_dist/piece.h"
#include "tpcc_dist/chopper.h"

// tpcc real dist partition benchmark
#include "tpcc_real_dist/piece.h"
#include "tpcc_real_dist/chopper.h"

// rw benchmark
#include "rw_benchmark/piece.h"
#include "rw_benchmark/chopper.h"

// micro bench
#include "micro/piece.h"
#include "micro/chopper.h"



namespace rococo {
Config *Config::config_s = NULL;


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
  unsigned int sid = 0, cid = 0;
  char *end_ptr    = NULL;

  char *hostspath               = NULL;
  char *ctrl_hostname           = NULL;
  char *ctrl_key                = NULL;
  char *ctrl_init               = NULL /*, *ctrl_run = NULL*/;
  unsigned int ctrl_port        = 0;
  unsigned int ctrl_timeout     = 0;
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
        cid = strtoul(optarg, &end_ptr, 10);
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

      // case 'r':
      //    ctrl_run = (char *)malloc((strlen(optarg) + 1) * sizeof(char));
      //    strcpy(ctrl_run, optarg);
      //    break;
      case 's': // site id
        // TODO remove
        sid = strtoul(optarg, &end_ptr, 10);

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
    server_or_client,
    logging_path);
  config_s->proc_name_ = proc_name;
  config_s->config_paths_ = config_paths;
  config_s->Load();
  return 0;
}

void Config::DestroyConfig() {
  if (config_s) {
    delete config_s;
    config_s = NULL;
  }
}

Config::Config() {}

Config::Config(char           *ctrl_hostname,
               uint32_t        ctrl_port,
               uint32_t        ctrl_timeout,
               char           *ctrl_key,
               char           *ctrl_init,
               uint32_t        duration,
               bool            heart_beat,
               single_server_t single_server,
               int32_t         server_or_client,
               string           logging_path) :
  ctrl_hostname_(ctrl_hostname),
  ctrl_port_(ctrl_port),
  ctrl_timeout_(ctrl_timeout),
  ctrl_key_(ctrl_key),
  ctrl_init_(ctrl_init) /*, ctrl_run_(ctrl_run)*/,
  duration_(duration),
  heart_beat_(heart_beat),
  single_server_(single_server),
  logging_path_(logging_path),
  retry_wait_(false) {

}

void Config::Load() {
  Sharding::sharding_s = new Sharding();
  for (auto &name: config_paths_) {
    if (boost::algorithm::ends_with(name, "xml")) {
      LoadXML(name);
    } else if (boost::algorithm::ends_with(name, "yml")) {
      LoadYML(name);
    } else {
      verify(0);
    }
  }

  // TODO particular configuration for certain workloads.
  this->InitTPCCD();
}

void Config::LoadYML(std::string &filename) {
//  YAML::Node config = YAML::LoadFile(name);

  YAML::Node config = YAML::LoadFile(filename);

  verify(Sharding::sharding_s);
//  Sharding::sharding_s = new Sharding();

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
}

void Config::LoadSiteYML(YAML::Node config) {
  auto servers = config["server"];
  parid_t par_id = 0;
  for (auto it = servers.begin(); it != servers.end(); it++) {
    auto par = *it;
    vector<string> v;
    for (auto iitt = par.begin(); iitt != par.end(); iitt++) {
      auto site_addr = iitt->as<string>();
      SiteInfo *info = new SiteInfo(next_site_id_++, site_addr);
      info->server_or_client_ = 0;
      info->par_id = par_id;
      site_infos_[info->name] = info;
      v.push_back(info->name);
    }
    par_servers_.push_back(v);
    par_id ++;
  }
  auto clients = config["client"];
  for (auto it = clients.begin(); it != clients.end(); it++) {
    auto par = *it;
    vector<string> v;
    for (auto iitt = par.begin(); iitt != par.end(); iitt++) {
      auto site_name = iitt->as<string>();
      SiteInfo *info = new SiteInfo(next_site_id_++);
      info->name = site_name;
      info->server_or_client_ = 1;
      site_infos_[info->name] = info;
      v.push_back(info->name);
    }
    par_clients_.push_back(v);
  }
}

void Config::LoadProcYML(YAML::Node config) {
  for (auto it = config.begin(); it != config.end(); it++) {
    auto site_name = it->first.as<string>();
    auto proc_name = it->second.as<string>();
    auto info = site_infos_[site_name];
    verify(info != nullptr);
    info->proc_name = proc_name;
  }
}

void Config::LoadHostYML(YAML::Node config) {
  for (auto it = config.begin(); it != config.end(); it++) {
    auto proc_name = it->first.as<string>();
    auto host_name = it->second.as<string>();
    proc_host_map_[proc_name] = host_name;
    for (auto &pair: site_infos_) {
      auto info = pair.second;
      if (info->proc_name == proc_name) {
        info->host = host_name;
      }
    }
  }
}

void Config::init_mode(std::string& mode_str) {
  auto it = modes_map_.find(mode_str);

  if (it != modes_map_.end()) {
    mode_ = it->second;
  } else {
    verify(0);
  }

  if ((mode_str == "rcc") || (mode_str == "deptran")) {
    // deprecated
    early_return_ = false;
  } else if (mode_str == "deptran_er") {
    // deprecated
    early_return_ = true;
  } else if (mode_str == "2pl_w") {
    retry_wait_ = true;
  } else if (mode_str == "2pl_wait_die") {
    mdb::FineLockedRow::set_wait_die();
  } else if ((mode_str == "2pl_ww") || (mode_str == "2pl_wound_die")) {
    mdb::FineLockedRow::set_wound_die();
  } 
}

void Config::init_bench(std::string& bench_str) {
  if (bench_str == "tpca") {
    benchmark_ = TPCA;
  } else if (bench_str == "tpcc") {
    benchmark_ = TPCC;
  } else if (bench_str == "tpcc_dist_part") {
    benchmark_ = TPCC_DIST_PART;
  } else if (bench_str == "tpccd") {
    benchmark_ = TPCC_REAL_DIST_PART;
  } else if (bench_str == "tpcc_real_dist_part") {
    benchmark_ = TPCC_REAL_DIST_PART;
  } else if (bench_str == "rw_benchmark") {
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


void Config::LoadXML(std::string &filename) {
  std::string filename_str(filename);
  boost::property_tree::ptree pt;
  boost::property_tree::xml_parser::read_xml(
    filename_str,
    pt,
    boost::property_tree::xml_parser::no_concat_text);
  verify(Sharding::sharding_s);
  // parse every table and its column
//  LoadModeXML(pt);
//  LoadTopoXML(pt);
//  LoadBenchXML(pt);
  LoadSchemeXML(pt);
}


void Config::LoadModeYML(YAML::Node config) {
  auto mode_str = config["cc"].as<string>();
  boost::algorithm::to_lower(mode_str);
  this->init_mode(mode_str);
  max_retry_ = config["retry"].as<int>();
  concurrent_txn_ = config["ongoing"].as<int>();
  batch_start_ = config["batch"].as<bool>();
}

void Config::LoadBenchYML(YAML::Node config) {
  std::string bench_str = config["workload"].as<string>();
  this->init_bench(bench_str);
  scale_factor_ = config["scale"].as<int>();
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
//  this->InitTPCCD();
}

void Config::InitTPCCD() {

  // TODO particular configuration for certain workloads.
  auto &tb_infos = Sharding::sharding_s->tb_info_;
  if (benchmark_ == TPCC_REAL_DIST_PART) {
    i32 n_w_id =
        (i32)(tb_infos[std::string(TPCC_TB_WAREHOUSE)].num_records);
    verify(n_w_id > 0);
    i32 n_d_id = (i32)(get_num_site() *
        tb_infos[std::string(TPCC_TB_DISTRICT)].num_records / n_w_id);
    i32 d_id = 0, w_id = 0;

    for (d_id = 0; d_id < n_d_id; d_id++)
      for (w_id = 0; w_id < n_w_id; w_id++) {
        MultiValue mv(std::vector<Value>({
                                             Value(d_id),
                                             Value(w_id)
                                         }));
        Sharding::sharding_s->insert_dist_mapping(mv);
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
        Sharding::sharding_s->insert_stock_mapping(mv);
      }
  }
}


void Config::LoadModeXML(boost::property_tree::ptree &pt) {
  // get mode
  std::string mode_str = pt.get<std::string>("benchmark.<xmlattr>.mode", "2PL");
  boost::algorithm::to_lower(mode_str);
  this->init_mode(mode_str);
}

void Config::LoadBenchXML(boost::property_tree::ptree &pt) {
  // get benchmark
  std::string bench_str = pt.get<std::string>("benchmark.<xmlattr>.name");
  this->init_bench(bench_str);
  scale_factor_   = pt.get<uint32_t>("benchmark.<xmlattr>.scale_factor", 1);
  max_retry_      = pt.get<uint32_t>("benchmark.<xmlattr>.max_retry", 1);
  concurrent_txn_ =
    pt.get<uint32_t>("benchmark.<xmlattr>.concurrent_txn", 1);
  batch_start_ = pt.get<bool>("benchmark.<xmlattr>.batch_start", false);
  // parse weight
  std::string txn_weight_str = pt.get<std::string>(
    "benchmark.<xmlattr>.txn_weight", "");
  size_t txn_weight_str_i = 0, end_txn_weight_str_i;
  char  *txn_weight_str_endptr;
  while (string::npos !=
         (end_txn_weight_str_i = txn_weight_str.find(':', txn_weight_str_i))) {
    double d = strtod(
      txn_weight_str.c_str() + txn_weight_str_i, &txn_weight_str_endptr);
    verify(txn_weight_str_endptr ==
           txn_weight_str.c_str() + end_txn_weight_str_i);
    txn_weight_str_i = end_txn_weight_str_i + 1;
    txn_weight_.push_back(d);
  }
  double d = strtod(
    txn_weight_str.c_str() + txn_weight_str_i, &txn_weight_str_endptr);
  verify(*txn_weight_str_endptr == '\0');
  txn_weight_.push_back(d);
}

void Config::LoadTopoXML(boost::property_tree::ptree &pt) {
  // TODO remove
  verify(0);
  // parse all the server sites
  num_site_     = pt.get<int>("benchmark.hosts.<xmlattr>.number");
//  site_         = (char **)malloc(sizeof(char *) * num_site_);
//  site_threads_ = (unsigned int *)malloc(sizeof(unsigned int) * num_site_);
  site_.resize(num_site_);
  site_threads_.resize(num_site_);
  unsigned int site_found = 0;
  BOOST_FOREACH(boost::property_tree::ptree::value_type const & value,
                pt.get_child("benchmark.hosts")) {
    if (value.first == "site") {
      int sid = value.second.get<int>("<xmlattr>.id");
      verify(sid < num_site_ && sid >= 0);

      // set site addr
      std::string siteaddr = value.second.get<std::string>("<xmltext>");
      std::string hostaddr = site2host_addr(siteaddr);
//      site_[sid] = (char *)malloc((hostaddr.size() + 1) * sizeof(char));
//      verify(site_[sid] != NULL);
//      strcpy(site_[sid], hostaddr.c_str());
        site_[sid] = hostaddr;

      // set site threads
      int threads = value.second.get<int>("<xmlattr>.threads");
      verify(threads > 0);
      site_threads_[sid] = (uint32_t)threads;
      site_found++;
    }
  }
  verify(site_found == num_site_);

  // parse all client sites
  int num_clients = pt.get<int>("benchmark.clients.<xmlattr>.number");
  verify(num_clients > 0);
  std::vector<bool> clients = vector<bool>(num_clients); 

  for (uint32_t client_index = 0; client_index < num_clients; client_index++) {
    clients[client_index] = false;
  }
  start_coordinator_id_ = 0;
  bool client_found = false;
  BOOST_FOREACH(
    boost::property_tree::ptree::value_type const & value,
    pt.get_child("benchmark.clients")
    ) {
    if (value.first == "client") {
      num_coordinator_threads_ = value.second.get<unsigned int>(
        "<xmlattr>.threads");
      std::string client_ids = value.second.get<std::string>("<xmlattr>.id");
      size_t pos             = client_ids.find('-');

      if (pos == std::string::npos) { // one client id
        char *end_ptr         = NULL;
        unsigned int find_cid = strtoul(client_ids.c_str(), &end_ptr, 10);
        verify(*end_ptr == '\0');
        verify(find_cid < num_clients);
        verify(!clients[find_cid]);
        clients[find_cid] = true;

        if (cid_ == find_cid) {
          client_found = true;
          break;
        }
        else {
          start_coordinator_id_ += num_coordinator_threads_;
        }
      } else { // client id formed as "0-9"
        std::string start = client_ids.substr(0, pos);
        verify(client_ids.size() > pos + 1);
        std::string end        = client_ids.substr(pos + 1);
        char *end_ptr          = NULL;
        uint32_t start_cid = strtoul(start.c_str(), &end_ptr, 10);
        verify(*end_ptr == '\0');
        uint32_t end_cid = strtoul(end.c_str(), &end_ptr, 10);
        verify(*end_ptr == '\0');
        verify(end_cid < num_clients && start_cid < end_cid);

        for (unsigned int mark_index = start_cid; mark_index <= end_cid;
             mark_index++) {
          verify(!clients[mark_index]);
          clients[mark_index] = true;
        }

        if ((cid_ <= end_cid) && (cid_ >= start_cid)) {
          start_coordinator_id_ += (cid_ - start_cid) *
                                   num_coordinator_threads_;
          client_found = true;
          break;
        } else {
          start_coordinator_id_ += (1 + end_cid - start_cid) *
                                   num_coordinator_threads_;
        }
      }
    }
  }
  verify(client_found);
}

void Config::LoadSchemeXML(boost::property_tree::ptree &pt) {
//  int *site_buf = (int *)malloc(sizeof(int) * num_site_);
  vector<int> site_buf(get_num_site());
  BOOST_FOREACH(boost::property_tree::ptree::value_type const & value,
                pt.get_child("benchmark")) {
    if (value.first == "table") {
      std::string tb_name = value.second.get<std::string>("<xmlattr>.name");

      if (Sharding::sharding_s->tb_info_.find(tb_name) !=
          Sharding::sharding_s->tb_info_.end()) verify(0);
      std::string method =
        value.second.get<std::string>("<xmlattr>.shard_method");
      uint64_t records = value.second.get<uint64_t>("<xmlattr>.records");
      Sharding::tb_info_t tb_info(method);
      tb_info.num_records = records * scale_factor_;
      verify(tb_info.num_records > 0);

      // set tb_info site info
      bool all_site = value.second.get<bool>("<xmlattr>.all_site", true);

      if (all_site) {
        tb_info.num_site = get_num_site();
        tb_info.site_id  =
          (uint32_t *)malloc(sizeof(uint32_t) * get_num_site());
        verify(tb_info.site_id != NULL);

        for (int i = 0; i < get_num_site(); i++) tb_info.site_id[i] = i;
      } else {
        verify(0);
      }
      verify(tb_info.num_site > 0 && tb_info.num_site <= get_num_site());

      // set tb_info.symbol TBL_SORTED or TBL_UNSORTED or TBL_SNAPSHOT
      std::string symbol_str = value.second.get<std::string>("<xmlattr>.type",
                                                             "sorted");
      auto it = tbl_types_map_.find(symbol_str);
      verify(it != tbl_types_map_.end());
      tb_info.symbol = it->second;

      // set tb_info.columns
      BOOST_FOREACH(boost::property_tree::ptree::value_type const & column,
                    value.second.get_child("schema")) {
        if (column.first == "column") {
          std::string c_type = column.second.get<std::string>("<xmlattr>.type");
          verify(c_type.size() > 0);
          Value::kind c_v_type;

          if (c_type == "i32") c_v_type = Value::I32;
          else if (c_type == "i64") c_v_type = Value::I64;
          else if (c_type == "double") c_v_type = Value::DOUBLE;
          else if (c_type == "str") c_v_type = Value::STR;
          else c_v_type = Value::UNKNOWN;

          std::string c_name = column.second.get<std::string>("<xmlattr>.name");
          verify(c_name.size() > 0);

          bool c_primary = column.second.get<bool>("<xmlattr>.primary", false);

          std::string c_foreign =
            column.second.get<std::string>("<xmlattr>.foreign", "");
          Sharding::column_t  *foreign_key_column = NULL;
          Sharding::tb_info_t *foreign_key_tb     = NULL;

          if (c_foreign.size() > 0) {
            size_t pos = c_foreign.find('.');
            verify(pos != std::string::npos);

            std::string foreign_tb = c_foreign.substr(0, pos);
            verify(c_foreign.size() > pos + 1);
            std::string foreign_column = c_foreign.substr(pos + 1);

            std::map<std::string,
                     Sharding::tb_info_t>::iterator ftb_it =
              Sharding::sharding_s->tb_info_.find(foreign_tb);
            verify(ftb_it != Sharding::sharding_s->tb_info_.end());
            foreign_key_tb = &(ftb_it->second);

            std::vector<Sharding::column_t>::iterator fc_it =
              ftb_it->second.columns.begin();

            for (; fc_it != ftb_it->second.columns.end(); fc_it++)
              if (fc_it->name == foreign_column) {
                verify(fc_it->type == c_v_type);

                if ((fc_it->values == NULL) &&
                    (server_or_client_ ==
                     0)) fc_it->values = new std::vector<Value>();
                foreign_key_column = &(*fc_it);
                break;
              }
            verify(fc_it != ftb_it->second.columns.end());
          }
          tb_info.columns.push_back(Sharding::column_t(c_v_type, c_name,
                                                       c_primary,
                                                       foreign_key_tb,
                                                       foreign_key_column));
        }
      }

      tb_info.tb_name                         = tb_name;
      Sharding::sharding_s->tb_info_[tb_name] = tb_info;
    }
  }
}

Config::~Config() {
  if (Sharding::sharding_s) {
    delete Sharding::sharding_s;
    Sharding::sharding_s = NULL;
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

  // if (ctrl_run_) {
  //    free(ctrl_run_);
  //    ctrl_run_ = NULL;
  // }
}

unsigned int Config::get_site_id() {
  return sid_;
}

unsigned int Config::get_client_id() {
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

// const char *Config::get_ctrl_run() {
//    return ctrl_run_;
// }
//
// TODO obsolete
int Config::get_all_site_addr(std::vector<std::string>& servers) {

  for (auto &pair : site_infos_) {
    auto info = pair.second;
    if (info->server_or_client_ == 0) {
      servers.push_back(info->GetHostAddr());
    }
  }
  return servers.size();
}

int Config::get_site_addr(unsigned int sid, std::string& server) {
  // TODO
  verify(0);
  if (site_.size() == 0) verify(0);

  if (sid >= num_site_) verify(0);

  std::string siteaddr(site_[sid]);

  //    std::string hostaddr = site2host_addr(siteaddr);
  server.assign(siteaddr);
  return 0;
}

//int Config::get_my_addr(std::string& server) {
//  if (site_.size() == 0) verify(0);
//
//  if (sid_ >= num_site_) verify(0);
//
//  server.assign("0.0.0.0:");
//  uint32_t len = site_[sid_].length(), p_start = 0;
//  uint32_t port_pos = site_[sid_].find_first_of(':') + 1;
//  verify(p_start < len && p_start > 0);
//  server.append(site_[sid_].substr(port_pos));
//  return 0;
//}

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
  return mode_;
}

unsigned int Config::get_num_threads() {
  return num_coordinator_threads_;
}

unsigned int Config::get_start_coordinator_id() {
  return start_coordinator_id_;
}

int Config::get_benchmark() {
  return benchmark_;
}

unsigned int Config::get_num_site() {
  return par_servers_.size();
}

unsigned int Config::get_scale_factor() {
  return scale_factor_;
}

unsigned int Config::get_max_retry() {
  return max_retry_;
}

Config::single_server_t Config::get_single_server() {
  return single_server_;
}

unsigned int Config::get_concurrent_txn() {
  return concurrent_txn_;
}

bool Config::get_batch_start() {
  return batch_start_;
}

std::vector<double>& Config::get_txn_weight() {
  return txn_weight_;
}

#ifdef CPU_PROFILE
int Config::get_prof_filename(char *prof_file) {
  if (prof_file == NULL) return -1;
  return sprintf(prof_file, "log/site-%d.prof", sid_);
}

#endif // ifdef CPU_PROFILE

bool Config::do_early_return() {
  return early_return_;
}

bool Config::do_logging() {
  return logging_path_.empty();
}

const char * Config::log_path() {
  return logging_path_.c_str();
}

bool Config::retry_wait() {
  return retry_wait_;
}
}
