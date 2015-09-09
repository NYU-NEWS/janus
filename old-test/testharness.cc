#include "base/all.hpp"
#include "deptran/all.h"

using namespace base;
using namespace rococo;

static char *create_argv(int &argc, char ** &argv) {
    char *argv_buf = (char *)malloc(500);
    strcpy(argv_buf, "./prog -f config/bch.xml -s 0 -h host -p 100 -k key -i init -r run -b -t 30 -d 1001");
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

int main(int argc, char* argv[]) {
    int dummy_argc;
    char **dummy_argv;
    char *pool = create_argv(dummy_argc, dummy_argv);
    Config::create_config(dummy_argc, dummy_argv);
    free(pool);
    return RUN_TESTS(argc, argv);
}
