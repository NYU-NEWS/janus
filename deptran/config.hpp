#ifndef CONFIG_H_
#define CONFIG_H_

#include <vector>
#include <string>

namespace rococo {

class SiteInfo {
public:
    static std::map<std::string, std::string> dns_;

    static std::string name2host(std::string& site_name);

    static void load_dns(std::string& hosts_path);

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
        std::string port_str = site_addr.substr(pos+1);
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

private:
    static Config *config_s;

    void init_sharding(const char *filename);
    void init_hostsmap(const char* hostspath);

    std::string site2host_addr(std::string& name);

    std::string site2host_name(std::string& addr);



    unsigned int cid_;
    unsigned int sid_;
    char *ctrl_hostname_;
    unsigned int ctrl_port_;
    unsigned int ctrl_timeout_;
    char *ctrl_key_;
    char *ctrl_init_;
    //char *ctrl_run_;
    unsigned int duration_;
    bool heart_beat_;
    char **site_;
    unsigned int *site_threads_;
    unsigned int num_site_;
    int mode_;
    unsigned int num_coordinator_threads_;
    unsigned int start_coordinator_id_;
    int benchmark_;
    unsigned int scale_factor_;
    unsigned int max_retry_;
    single_server_t single_server_;
    unsigned int concurrent_txn_;
    bool batch_start_;
    int server_or_client_; // 0 for server, 1 for client, init -1
    std::vector<double> txn_weight_;
    bool early_return_;
    char *logging_path_;
    bool retry_wait_;

public:
    std::map<string, string> hostsmap_;

protected:
    Config();

    Config(unsigned int cid, 
            unsigned int sid, 
            char *filename, 
            char *ctrl_hostname, 
            unsigned int ctrl_port, 
            unsigned int ctrl_timeout, 
            char *ctrl_key, 
            char *ctrl_init
	   /*, char *ctrl_run*/, 
            unsigned int duration, 
            bool heart_beat, 
            single_server_t single_server, 
            int server_or_client, 
            char *logging_path,
            char *hostspath
            );

public:
    static int create_config(int argc, char *argv[]);

    static Config *get_config();

    static void destroy_config();

    unsigned int get_site_id();

    unsigned int get_client_id();

    unsigned int get_ctrl_port();

    unsigned int get_ctrl_timeout();

    const char *get_ctrl_hostname();

    const char *get_ctrl_key();

    const char *get_ctrl_init();

    //const char *get_ctrl_run();

    unsigned int get_duration();

    bool do_heart_beat();

    int get_all_site_addr(std::vector<std::string> &servers);

    int get_site_addr(unsigned int sid, std::string &server);

    int get_my_addr(std::string &server);

    int get_threads(unsigned int &threads);

    int get_mode();

    unsigned int get_num_threads();

    unsigned int get_start_coordinator_id();

    int get_benchmark();

    unsigned int get_num_site();

    unsigned int get_scale_factor();

    unsigned int get_max_retry();

    single_server_t get_single_server();

    unsigned int get_concurrent_txn();

    bool get_batch_start();

    bool do_early_return();

#ifdef CPU_PROFILE
    int get_prof_filename(char *prof_file);
#endif

    bool do_logging();

    char *log_path();

    bool retry_wait();

    std::vector<double> &get_txn_weight();

    ~Config();
};

}

#endif
