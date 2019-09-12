#include "deptran/s_main.h"

int main(int argc, char* argv[]) {
  int ret = setup(argc, argv);
  if (ret != 0) {
    return ret;
  }
  microbench_paxos();
  return shutdown_paxos();
}