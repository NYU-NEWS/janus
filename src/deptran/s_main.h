#pragma once

#include <functional>

int setup(int argc, char* argv[]);
int shutdown_paxos();
void microbench_paxos();
void register_for_follower(std::function<void(const char*, int)>, uint32_t);
void register_for_leader(std::function<void(const char*, int)>, uint32_t);
void submit(const char*, int, uint32_t);
void wait_for_submit(uint32_t);
void microbench_paxos_queue();