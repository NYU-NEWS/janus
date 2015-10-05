#include "all.h"
#include <unistd.h>
#include <signal.h>

namespace deptran {

bool TimerLoop::run_ = false;
pthread_mutex_t TimerLoop::run_mutex_ = PTHREAD_MUTEX_INITIALIZER;

void TimerLoop::set_alarm(unsigned int time) {
  struct sigaction sact;
  sigemptyset(&sact.sa_mask);
  sact.sa_flags = 0;
  sact.sa_handler = alarm_handler;
  sigaction(SIGALRM, &sact, NULL);
  alarm(time);
}

void TimerLoop::init(uint32_t time) {
  pthread_mutex_lock(&run_mutex_);
  if (!run_) {
    run_ = true;
    set_alarm((unsigned int) time);
  }
  pthread_mutex_unlock(&run_mutex_);
}

void TimerLoop::alarm_handler(int sig) {
  run_ = false;
}

bool TimerLoop::is_run() {
  return run_;
}

}
