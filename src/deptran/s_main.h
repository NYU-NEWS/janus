#pragma once

#include <functional>

int setup(int argc, char* argv[]);
int shutdown_paxos();
void microbench_paxos();
void register_for_follower(std::function<void(const char*, int)>);
void register_for_leader(std::function<void(const char*, int)>);
void submit(const char*, int);
void wait_for_submit();
void microbench_paxos_queue();