#include "all.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <boost/foreach.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>

namespace rococo {
Config *Config::config_s = NULL;

std::map<std::string, std::string> SiteInfo::dns_;

std::string SiteInfo::name2host(std::string& site_name) {
  Log::debug("find host name by site name: %s", site_name.c_str());
  auto it = SiteInfo::dns_.find(site_name);

  if (it != SiteInfo::dns_.end()) {
    return it->second;
  } else {
    Log::debug("did not find a mapping to site_name: %s", site_name.c_str());
    return site_name;
  }
}

void SiteInfo::load_dns(std::string& hosts_path) {
  std::string   host_name, site_name;
  std::ifstream in(hosts_path);

  while (in) {
    in >> host_name;
    in >> site_name;
    dns_[site_name] = host_name;
  }
}

Config * Config::get_config() {
  if (config_s == NULL) config_s = new Config();
  return config_s;
}

int Config::create_config(int argc, char *argv[]) {
  if (config_s != NULL) return -1;

  std::string  filename;
  unsigned int sid = 0, cid = 0;
  char *end_ptr    = NULL;

  char *hostspath               = NULL;
  char *ctrl_hostname           = NULL;
  char *ctrl_key                = NULL;
  char *ctrl_init               = NULL /*, *ctrl_run = NULL*/;
  char *logging_path            = NULL;
  unsigned int ctrl_port        = 0;
  unsigned int ctrl_timeout     = 0;
  unsigned int duration         = 0;
  bool heart_beat               = false;
  single_server_t single_server = SS_DISABLED;
  int server_or_client          = -1;

  int c;
  optind = 1;

  while ((c = getopt(argc, argv, "bc:d:f:h:i:k:p:r:s:S:t:H:")) != -1) {
    switch (c) {
      case 'b': // heartbeat to controller
        heart_beat = true;
        break;
      case 'c': // client id
        cid = strtoul(optarg, &end_ptr, 10);
        if ((end_ptr == NULL) || (*end_ptr != '\0'))
          return -4;
        if (server_or_client != -1)
          return -4;
        server_or_client = 1;
        break;
      case 'd': // duration
        duration = strtoul(optarg, &end_ptr, 10);

        if ((end_ptr == NULL) || (*end_ptr != '\0'))
          return -4;
        break;
      case 'f': // properties.xml
        filename = std::string(optarg);
        break;
      case 'h': // ctrl_hostname
        ctrl_hostname = (char *)malloc((strlen(optarg) + 1) * sizeof(char));
        strcpy(ctrl_hostname, optarg);
        break;
      case 'H': // ctrl_host
        hostspath = (char *)malloc((strlen(optarg) + 1) * sizeof(char));
        strcpy(hostspath, optarg);
        break;
      case 'i': // ctrl_init
        ctrl_init = (char *)malloc((strlen(optarg) + 1) * sizeof(char));
        strcpy(ctrl_init, optarg);
        break;
      case 'k':
        ctrl_key = (char *)malloc((strlen(optarg) + 1) * sizeof(char));
        strcpy(ctrl_key, optarg);
        break;
      case 'r': // logging path
        logging_path = (char *)malloc((strlen(optarg) + 1) * sizeof(char));
        strcpy(logging_path, optarg);
        break;
      case 'p':
        ctrl_port = strtoul(optarg, &end_ptr, 10);

        if ((end_ptr == NULL) || (*end_ptr != '\0')) return -4;
        break;

      // case 'r':
      //    ctrl_run = (char *)malloc((strlen(optarg) + 1) * sizeof(char));
      //    strcpy(ctrl_run, optarg);
      //    break;
      case 's': // site id
        sid = strtoul(optarg, &end_ptr, 10);

        if ((end_ptr == NULL) || (*end_ptr != '\0')) return -4;

        if (server_or_client != -1) return -4;
        server_or_client = 0;
        break;
      case 'S': // client touch only single server
      {
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
      case 't':
        ctrl_timeout = strtoul(optarg, &end_ptr, 10);

        if ((end_ptr == NULL) || (*end_ptr != '\0')) return -4;
        break;
      case '?':

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

  if ((server_or_client != 0) && (server_or_client != 1)) return -5;
  config_s = new Config(
    cid,
    sid,
    filename,
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
    logging_path,
    hostspath);
  return 0;
}

void Config::destroy_config() {
  if (config_s) {
    delete config_s;
    config_s = NULL;
  }
}

Config::Config() {}

Config::Config(uint32_t        cid,
               uint32_t        sid,
               std::string   & filename,
               char           *ctrl_hostname,
               uint32_t        ctrl_port,
               uint32_t        ctrl_timeout,
               char           *ctrl_key,
               char           *ctrl_init,
               uint32_t        duration,
               bool            heart_beat,
               single_server_t single_server,
               int32_t         server_or_client,
               char           *logging_path,
               char           *hostspath) :
  cid_(cid),
  sid_(sid),
  ctrl_hostname_(ctrl_hostname),
  ctrl_port_(ctrl_port),
  ctrl_timeout_(ctrl_timeout),
  ctrl_key_(ctrl_key),
  ctrl_init_(ctrl_init) /*, ctrl_run_(ctrl_run)*/,
  duration_(duration),
  heart_beat_(heart_beat),
  single_server_(single_server),
  server_or_client_(server_or_client),
  logging_path_(logging_path),
  retry_wait_(false) {
  if (hostspath != NULL) {
    init_hostsmap(hostspath);
  }

  std::string configs[2] = { filename, "./config/sample.yml" };

  for (auto& name: configs) {
    if (boost::algorithm::ends_with(name, "yml")) {
      load_config_yml(name);
    } else if (boost::algorithm::ends_with(name, "xml")) {
      load_config_xml(name);
    } else {
      verify(0);
    }
  }
}

void Config::load_config_yml(std::string& name) {
  YAML::Node config = YAML::LoadFile(name);
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
  } else {
    // mode not supported.
    verify(0);
  }
}

void Config::init_hostsmap(const char *hostspath) {
  std::string   hostname, sitename;
  std::ifstream in(hostspath);

  while (in) {
    in >> hostname;
    in >> sitename;
    hostsmap_[sitename] = hostname;
  }
}

void Config::init_bench(std::string& bench_str) {
  if (bench_str == "tpca") {
    benchmark_ = TPCA;
  } else if (bench_str == "tpcc") {
    benchmark_ = TPCC;
  } else if (bench_str == "tpcc_dist_part") {
    benchmark_ = TPCC_DIST_PART;
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
  auto it = hostsmap_.find(sitename);

  if (it != hostsmap_.end()) {
    return it->second;
  } else {
    return sitename;
  }
}

void Config::load_config_xml(std::string& filename) {
  std::string filename_str(filename);
  boost::property_tree::ptree pt;
  boost::property_tree::xml_parser::read_xml(
    filename_str,
    pt,
    boost::property_tree::xml_parser::no_concat_text);
  Sharding::sharding_s = new Sharding();
  // get mode
  std::string mode_str = pt.get<std::string>("benchmark.<xmlattr>.mode", "2PL");
  boost::algorithm::to_lower(mode_str);
  this->init_mode(mode_str);
  // get benchmark
  std::string bench_str = pt.get<std::string>("benchmark.<xmlattr>.name");
  this->init_bench(bench_str);
  scale_factor_   = pt.get<unsigned int>("benchmark.<xmlattr>.scale_factor", 1);
  max_retry_      = pt.get<unsigned int>("benchmark.<xmlattr>.max_retry", 1);
  concurrent_txn_ =
    pt.get<unsigned int>("benchmark.<xmlattr>.concurrent_txn", 1);
  batch_start_ = pt.get<bool>("benchmark.<xmlattr>.batch_start", false);
  std::string txn_weight_str = pt.get<std::string>(
    "benchmark.<xmlattr>.txn_weight", "");
  // parse weight
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

  // parse all the server sites
  num_site_     = pt.get<int>("benchmark.hosts.<xmlattr>.number");
  site_         = (char **)malloc(sizeof(char *) * num_site_);
  site_threads_ = (unsigned int *)malloc(sizeof(unsigned int) * num_site_);
  verify(site_ != NULL);
  unsigned int site_found = 0;
  memset(site_,         0, sizeof(char *) * num_site_);
  memset(site_threads_, 0, sizeof(unsigned int) * num_site_);
  BOOST_FOREACH(boost::property_tree::ptree::value_type const & value,
                pt.get_child("benchmark.hosts")) {
    if (value.first == "site") {
      int sid = value.second.get<int>("<xmlattr>.id");
      verify(sid < num_site_ && sid >= 0);
      verify(site_[sid] == NULL);

      // set site addr
      std::string siteaddr = value.second.get<std::string>("<xmltext>");
      std::string hostaddr = site2host_addr(siteaddr);
      site_[sid] = (char *)malloc((hostaddr.size() + 1) * sizeof(char));
      verify(site_[sid] != NULL);
      strcpy(site_[sid], hostaddr.c_str());

      // set site threads
      int threads = value.second.get<int>("<xmlattr>.threads");
      verify(threads > 0);
      site_threads_[sid] = (unsigned int)threads;
      site_found++;
    }
  }
  verify(site_found == num_site_);

  // parse all client sites
  int num_clients = pt.get<int>("benchmark.clients.<xmlattr>.number");
  verify(num_clients > 0);
  bool *clients = (bool *)malloc(sizeof(bool) * num_clients);

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
        unsigned int start_cid = strtoul(start.c_str(), &end_ptr, 10);
        verify(*end_ptr == '\0');
        unsigned int end_cid = strtoul(end.c_str(), &end_ptr, 10);
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
  free(clients);
  verify(client_found);

  // parse every table and its column
  int *site_buf = (int *)malloc(sizeof(int) * num_site_);
  verify(site_buf != NULL);
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
      verify(records > 0);

      // set tb_info site info
      bool all_site = value.second.get<bool>("<xmlattr>.all_site", true);

      if (all_site) {
        tb_info.num_site = num_site_;
        tb_info.site_id  =
          (unsigned int *)malloc(sizeof(unsigned int) * num_site_);
        verify(tb_info.site_id != NULL);

        for (int i = 0; i < num_site_; i++) tb_info.site_id[i] = i;
      } else {
        verify(0);
        memset(site_buf, 0, sizeof(int) * num_site_);
        unsigned int num_site_buf = 0;
        BOOST_FOREACH(
          boost::property_tree::ptree::value_type const & site_value,
          value.second.get_child("site")) {
          if (site_value.first == "site_id") {
            int sid = site_value.second.get<int>("<xmltext>");
            verify(sid < num_site_ && sid >= 0);
            verify(site_buf[sid] == 0);

            if (site_buf[sid] == 0) num_site_buf++;
            site_buf[sid] = 1;
          }
        }
        tb_info.num_site = num_site_buf;
        tb_info.site_id  = (unsigned int *)malloc(
          sizeof(unsigned int) * num_site_buf);
        verify(tb_info.site_id != NULL);
        unsigned int j = 0;

        for (unsigned int i = 0; i < num_site_; i++)
          if (site_buf[i] == 1) tb_info.site_id[j++] = i;
        verify(j == num_site_buf);
      }
      verify(tb_info.num_site > 0 && tb_info.num_site <= num_site_);

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

  // TODO particular configuration for certain workloads.
  if (benchmark_ == TPCC_REAL_DIST_PART) {
    i32 n_w_id =
      (i32)(Sharding::sharding_s->tb_info_[std::string(TPCC_TB_WAREHOUSE)].
            num_records);
    i32 n_d_id =
      (i32)(num_site_ *
            Sharding::sharding_s->tb_info_[std::string(TPCC_TB_DISTRICT)].
            num_records /
            n_w_id);
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
      (i32)(Sharding::sharding_s->tb_info_[std::string(TPCC_TB_ITEM)].
            num_records);
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
  free(site_buf);
}

Config::~Config() {
  if (Sharding::sharding_s) {
    delete Sharding::sharding_s;
    Sharding::sharding_s = NULL;
  }

  if (site_) {
    for (unsigned int i = 0; i < num_site_; i++)
      if (site_[i]) free(site_[i]);
    free(site_);
    site_ = NULL;
  }

  if (site_threads_) {
    free(site_threads_);
    site_threads_ = NULL;
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
  if (logging_path_) {
    free(logging_path_);
    logging_path_ = NULL;
  }
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

int Config::get_all_site_addr(std::vector<std::string>& servers) {
  if (site_ == NULL) return -1;
  unsigned int i = 0;

  for (; i < num_site_; i++) {
    if (site_[i] == NULL) {
      servers.clear();
      return -2;
    }
    std::string siteaddr(site_[i]);

    // std::string hostaddr = site2host_addr(siteaddr);
    //        Log::info("site address: %s", hostaddr.c_str());
    servers.push_back(siteaddr);
  }
  return servers.size();
}

int Config::get_site_addr(unsigned int sid, std::string& server) {
  if (site_ == NULL) return -1;

  if (sid >= num_site_) return -2;

  if (site_[sid] == NULL) return -3;
  std::string siteaddr(site_[sid]);

  //    std::string hostaddr = site2host_addr(siteaddr);
  server.assign(siteaddr);
  return 0;
}

int Config::get_my_addr(std::string& server) {
  if (site_ == NULL) return -1;

  if (sid_ >= num_site_) return -2;

  if (site_[sid_] == NULL) return -3;
  server.assign("0.0.0.0:");
  unsigned int len = strlen(site_[sid_]), p_start = 0;

  for (unsigned int i = 0; i < len; i++) {
    if (site_[sid_][i] == ':') {
      p_start = i + 1;
      break;
    }
  }
  verify(p_start < len && p_start > 0);
  server.append(site_[sid_] + p_start);
  return 0;
}

int Config::get_threads(unsigned int& threads) {
  if (site_threads_ == NULL) return -1;

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
  return num_site_;
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
  return logging_path_ != NULL;
}

char * Config::log_path() {
  return logging_path_;
}

bool Config::retry_wait() {
  return retry_wait_;
}
}
