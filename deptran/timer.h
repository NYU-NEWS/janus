#ifndef TIMER_LOOP_H_
#define TIMER_LOOP_H_

#define TIMER_LOOP(t) TimerLoop::init(t); while (TimerLoop::is_run())
#define TIMER_SET(t) TimerLoop::init(t)
#define TIMER_IF_NOT_END if (TimerLoop::is_run())

#include <signal.h>

namespace deptran {

class TimerLoop {
 private:
  static void alarm_handler(int sig);
  static void set_alarm(unsigned int time);
  static bool run_;
  static pthread_mutex_t run_mutex_;

 public:
  static void init(uint32_t time = 60);
  static bool is_run();
};

}

#endif
