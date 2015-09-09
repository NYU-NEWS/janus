
#include "base/all.hpp"

#include "deptran/all.h"

using namespace rococo;

static char *create_argv(int &argc, char ** &argv) {
    char *argv_buf = (char *)malloc(500);
    strcpy(argv_buf, "./prog -f config/tst.xml -s 2 -h host -p 100 -k key -i init -r run -b -t 30 -d 1001");
    argv = (char **)malloc(sizeof(char*) * 20);
    argv[0] = argv_buf;
    argv_buf[6] = '\0';
    argv[1] = argv_buf + 7;
    argv_buf[9] = '\0';
    argv[2] = argv_buf + 10;
    argv_buf[24] = '\0';
    argv[3] = argv_buf + 25;
    argv_buf[27] = '\0';
    argv[4] = argv_buf + 28;
    argv_buf[29] = '\0';
    argv[5] = argv_buf + 30;
    argv_buf[32] = '\0';
    argv[6] = argv_buf + 33;
    argv_buf[37] = '\0';
    argv[7] = argv_buf + 38;
    argv_buf[40] = '\0';
    argv[8] = argv_buf + 41;
    argv_buf[44] = '\0';
    argv[9] = argv_buf + 45;
    argv_buf[47] = '\0';
    argv[10] = argv_buf + 48;
    argv_buf[51] = '\0';
    argv[11] = argv_buf + 52;
    argv_buf[54] = '\0';
    argv[12] = argv_buf + 55;
    argv_buf[59] = '\0';
    argv[13] = argv_buf + 60;
    argv_buf[62] = '\0';
    argv[14] = argv_buf + 63;
    argv_buf[66] = '\0';
    argv[15] = argv_buf + 67;
    argv_buf[69] = '\0';
    argv[16] = argv_buf + 70;
    argv_buf[72] = '\0';
    argv[17] = argv_buf + 73;
    argv_buf[75] = '\0';
    argv[18] = argv_buf + 76;
    argv_buf[78] = '\0';
    argv[19] = argv_buf + 79;

    argc = 20;
    return argv_buf;
}

TEST(Config, get_xx) {
    Config::destroy_config();
    int argc;
    char **argv;
    char *pool = create_argv(argc, argv);
    int ret = Config::create_config(argc, argv);
    EXPECT_EQ(0, ret);

    unsigned int sid = Config::get_config()->get_site_id();
    EXPECT_EQ(2, sid);

    EXPECT_EQ(std::string(Config::get_config()->get_ctrl_hostname()), "host");
    EXPECT_EQ(Config::get_config()->get_ctrl_port(), 100);
    EXPECT_EQ(std::string(Config::get_config()->get_ctrl_key()), "key");
    EXPECT_EQ(std::string(Config::get_config()->get_ctrl_init()), "init");
    //EXPECT_EQ(std::string(Config::get_config()->get_ctrl_run()), "run");
    EXPECT_EQ(Config::get_config()->get_ctrl_timeout(), 30);
    EXPECT_TRUE(Config::get_config()->do_heart_beat());
    EXPECT_EQ(Config::get_config()->get_duration(), 1001);
    EXPECT_EQ(MODE_2PL, Config::get_config()->get_mode());
    EXPECT_EQ(TPCA, Config::get_config()->get_benchmark());
    EXPECT_EQ(3, Config::get_config()->get_scale_factor());
    EXPECT_EQ(2, Config::get_config()->get_max_retry());

    free(pool);
    free(argv);
    Config::destroy_config();
}

TEST(Config, get_site_id) {
    Config::destroy_config();
    int argc;
    char **argv;
    char *pool = create_argv(argc, argv);
    int ret = Config::create_config(argc, argv);
    EXPECT_EQ(0, ret);

    unsigned int sid = Config::get_config()->get_site_id();
    EXPECT_EQ(2, sid);

    free(pool);
    free(argv);
    Config::destroy_config();
}

TEST(Config, get_threads) {
    Config::destroy_config();
    int argc;
    char **argv;
    char *pool = create_argv(argc, argv);
    int ret = Config::create_config(argc, argv);
    EXPECT_EQ(0, ret);

    unsigned int num_threads;
    ret = Config::get_config()->get_threads(num_threads);
    EXPECT_EQ(0, ret);
    EXPECT_EQ(1, num_threads);

    free(pool);
    free(argv);
    Config::destroy_config();
}

TEST(Config, get_all_site_addr) {
    Config::destroy_config();
    int argc;
    char **argv;
    char *pool = create_argv(argc, argv);
    int ret = Config::create_config(argc, argv);
    EXPECT_EQ(0, ret);

    std::vector<std::string> servers;
    EXPECT_EQ(3, Config::get_config()->get_all_site_addr(servers));
    EXPECT_EQ(servers[0], "localhost:3333");
    EXPECT_EQ(servers[1], "localhost:3334");
    EXPECT_EQ(servers[2], "localhost:3335");

    free(pool);
    free(argv);
    Config::destroy_config();
}

TEST(Config, get_site_addr) {
    Config::destroy_config();
    int argc;
    char **argv;
    char *pool = create_argv(argc, argv);
    int ret = Config::create_config(argc, argv);
    EXPECT_EQ(0, ret);

    std::string server;
    ret = Config::get_config()->get_site_addr(0, server);
    EXPECT_EQ(0, ret);
    EXPECT_EQ(server, "localhost:3333");
    ret = Config::get_config()->get_site_addr(1, server);
    EXPECT_EQ(0, ret);
    EXPECT_EQ(server, "localhost:3334");
    ret = Config::get_config()->get_site_addr(2, server);
    EXPECT_EQ(0, ret);
    EXPECT_EQ(server, "localhost:3335");

    free(pool);
    free(argv);
    Config::destroy_config();
}

TEST(Config, get_my_addr) {
    Config::destroy_config();
    int argc;
    char **argv;
    char *pool = create_argv(argc, argv);
    int ret = Config::create_config(argc, argv);
    EXPECT_EQ(0, ret);

    std::string server;
    ret = Config::get_config()->get_my_addr(server);
    EXPECT_EQ(0, ret);
    EXPECT_EQ(server, "0.0.0.0:3335");

    free(pool);
    free(argv);
    Config::destroy_config();
}

TEST(Config, get_txn_weight) {
    Config::destroy_config();
    int argc;
    char **argv;
    char *pool = create_argv(argc, argv);
    int ret = Config::create_config(argc, argv);
    EXPECT_EQ(0, ret);

    std::vector<double> &txn_weight = Config::get_config()->get_txn_weight();
    EXPECT_EQ(txn_weight.size(), 5);
    double e = 0.001;
    EXPECT_LT(txn_weight[0] - 0, e);
    EXPECT_LT(0 - txn_weight[0], e);

    EXPECT_LT(txn_weight[1] - 99, e);
    EXPECT_LT(99 - txn_weight[1], e);

    EXPECT_LT(txn_weight[2] - 60, e);
    EXPECT_LT(60 - txn_weight[2], e);

    EXPECT_LT(txn_weight[3] - 10.3, e);
    EXPECT_LT(10.3 - txn_weight[3], e);

    EXPECT_LT(txn_weight[4] - 13.5, e);
    EXPECT_LT(13.5 - txn_weight[4], e);

    free(pool);
    free(argv);
    Config::destroy_config();
}

TEST(Sharding, get_site_id) {
    Config::destroy_config();
    int argc;
    char **argv;
    char *pool = create_argv(argc, argv);
    int ret = Config::create_config(argc, argv);
    EXPECT_EQ(0, ret);

    unsigned int sid;
    Value key((i32)102);
    EXPECT_EQ(0, Sharding::get_site_id("branch", key, sid));
    EXPECT_EQ(sid, 1);
    EXPECT_EQ(0, Sharding::get_site_id("teller", key, sid));
    EXPECT_EQ(sid, 0);
    EXPECT_EQ(0, Sharding::get_site_id("customer", key, sid));
    EXPECT_EQ(sid, 0);

    free(pool);
    free(argv);
    Config::destroy_config();
}

TEST(Sharding, get_table_names) {
    Config::destroy_config();
    int argc;
    char **argv;
    char *pool = create_argv(argc, argv);
    int ret = Config::create_config(argc, argv);
    EXPECT_EQ(0, ret);

    std::vector<std::string> tables;
    EXPECT_EQ(3, Sharding::get_table_names(2, tables));

    EXPECT_EQ(2, Sharding::get_table_names(0, tables));

    free(pool);
    free(argv);
    Config::destroy_config();
}

TEST(Sharding, init_schema) {
    Config::destroy_config();
    int argc;
    char **argv;
    char *pool = create_argv(argc, argv);
    int ret = Config::create_config(argc, argv);
    EXPECT_EQ(0, ret);

    mdb::Schema schema1, schema2, schema3, schema4, schema5, schema6;

    mdb::symbol_t symbol;
    EXPECT_EQ(2, Sharding::init_schema(std::string("branch"), &schema1, &symbol));
    EXPECT_EQ(mdb::TBL_SORTED, symbol);
    EXPECT_EQ(2, Sharding::init_schema("branch", &schema2, &symbol));
    EXPECT_EQ(mdb::TBL_SORTED, symbol);
    EXPECT_EQ(3, Sharding::init_schema(std::string("teller"), &schema3, &symbol));
    EXPECT_EQ(mdb::TBL_UNSORTED, symbol);
    EXPECT_EQ(3, Sharding::init_schema("teller", &schema4, &symbol));
    EXPECT_EQ(mdb::TBL_UNSORTED, symbol);
    EXPECT_EQ(3, Sharding::init_schema(std::string("customer"), &schema5, &symbol));
    EXPECT_EQ(mdb::TBL_SNAPSHOT, symbol);
    EXPECT_EQ(3, Sharding::init_schema("customer", &schema6, &symbol));
    EXPECT_EQ(mdb::TBL_SNAPSHOT, symbol);

    free(pool);
    free(argv);
    Config::destroy_config();
}

TEST(Sharding, index_increase) {
    std::map<unsigned int, std::pair<unsigned int, unsigned int> > index;
    index[0] = std::pair<unsigned int, unsigned int>(3, 3);
    index[2] = std::pair<unsigned int, unsigned int>(3, 2);
    index[5] = std::pair<unsigned int, unsigned int>(3, 5);
    int ret;
    ret = init_index(index);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(index[0].first, 0);
    EXPECT_EQ(index[2].first, 0);
    EXPECT_EQ(index[5].first, 0);
    EXPECT_EQ(index[0].second, 3);
    EXPECT_EQ(index[2].second, 2);
    EXPECT_EQ(index[5].second, 5);
    for (int i = 0; i < 3; i++)
        index_increase(index);

    // 0; 1; 0 ==> 3
    EXPECT_EQ(index[0].first, 0);
    EXPECT_EQ(index[2].first, 1);
    EXPECT_EQ(index[5].first, 0);
    EXPECT_EQ(index[0].second, 3);
    EXPECT_EQ(index[2].second, 2);
    EXPECT_EQ(index[5].second, 5);

    for (int i = 0; i < 25; i++)
        index_increase(index);

    // 4; 1; 1 ==> 28
    EXPECT_EQ(index[0].first, 1);
    EXPECT_EQ(index[2].first, 1);
    EXPECT_EQ(index[5].first, 4);
    EXPECT_EQ(index[0].second, 3);
    EXPECT_EQ(index[2].second, 2);
    EXPECT_EQ(index[5].second, 5);

    ret = index_increase(index);
    EXPECT_EQ(ret, 0);
    // 4; 1; 2 ==> 29
    EXPECT_EQ(index[0].first, 2);
    EXPECT_EQ(index[2].first, 1);
    EXPECT_EQ(index[5].first, 4);
    EXPECT_EQ(index[0].second, 3);
    EXPECT_EQ(index[2].second, 2);
    EXPECT_EQ(index[5].second, 5);

    ret = index_increase(index);
    EXPECT_EQ(ret, -1);
    // overflow
    EXPECT_EQ(index[0].first, 0);
    EXPECT_EQ(index[2].first, 0);
    EXPECT_EQ(index[5].first, 0);
    EXPECT_EQ(index[0].second, 3);
    EXPECT_EQ(index[2].second, 2);
    EXPECT_EQ(index[5].second, 5);

}

TEST(Sharding, index_increase_bound) {
    std::map<unsigned int, std::pair<unsigned int, unsigned int> > index;
    index[0] = std::pair<unsigned int, unsigned int>(3, 3);
    index[2] = std::pair<unsigned int, unsigned int>(3, 2);
    index[5] = std::pair<unsigned int, unsigned int>(3, 5);
    index[1] = std::pair<unsigned int, unsigned int>(3, 5);
    index[3] = std::pair<unsigned int, unsigned int>(3, 5);
    std::vector<unsigned int> bound({5, 1, 3});
    int ret;
    ret = init_index(index);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(index[0].first, 0);
    EXPECT_EQ(index[2].first, 0);
    EXPECT_EQ(index[5].first, 0);
    EXPECT_EQ(index[1].first, 0);
    EXPECT_EQ(index[3].first, 0);
    EXPECT_EQ(index[0].second, 3);
    EXPECT_EQ(index[2].second, 2);
    EXPECT_EQ(index[5].second, 5);
    EXPECT_EQ(index[1].second, 5);
    EXPECT_EQ(index[3].second, 5);
    for (int i = 0; i < 3; i++)
        index_increase(index, bound);

    // 0; 1; 0 ==> 3
    EXPECT_EQ(index[0].first, 0);
    EXPECT_EQ(index[2].first, 1);
    EXPECT_EQ(index[5].first, 0);
    EXPECT_EQ(index[1].first, 0);
    EXPECT_EQ(index[3].first, 0);
    EXPECT_EQ(index[0].second, 3);
    EXPECT_EQ(index[2].second, 2);
    EXPECT_EQ(index[5].second, 5);
    EXPECT_EQ(index[1].second, 5);
    EXPECT_EQ(index[3].second, 5);

    for (int i = 0; i < 25; i++)
        index_increase(index, bound);

    // 4; 1; 1 ==> 28
    EXPECT_EQ(index[0].first, 1);
    EXPECT_EQ(index[2].first, 1);
    EXPECT_EQ(index[5].first, 4);
    EXPECT_EQ(index[1].first, 4);
    EXPECT_EQ(index[3].first, 4);
    EXPECT_EQ(index[0].second, 3);
    EXPECT_EQ(index[2].second, 2);
    EXPECT_EQ(index[5].second, 5);
    EXPECT_EQ(index[1].second, 5);
    EXPECT_EQ(index[3].second, 5);

    ret = index_increase(index, bound);
    EXPECT_EQ(ret, 0);
    // 4; 1; 2 ==> 29
    EXPECT_EQ(index[0].first, 2);
    EXPECT_EQ(index[2].first, 1);
    EXPECT_EQ(index[5].first, 4);
    EXPECT_EQ(index[1].first, 4);
    EXPECT_EQ(index[3].first, 4);
    EXPECT_EQ(index[0].second, 3);
    EXPECT_EQ(index[2].second, 2);
    EXPECT_EQ(index[5].second, 5);
    EXPECT_EQ(index[1].second, 5);
    EXPECT_EQ(index[3].second, 5);

    ret = index_increase(index, bound);
    EXPECT_EQ(ret, -1);
    // overflow
    EXPECT_EQ(index[0].first, 0);
    EXPECT_EQ(index[2].first, 0);
    EXPECT_EQ(index[5].first, 0);
    EXPECT_EQ(index[1].first, 0);
    EXPECT_EQ(index[3].first, 0);
    EXPECT_EQ(index[0].second, 3);
    EXPECT_EQ(index[2].second, 2);
    EXPECT_EQ(index[5].second, 5);
    EXPECT_EQ(index[1].second, 5);
    EXPECT_EQ(index[3].second, 5);

}
