
#include "../__dep__.h"
#include "../../rrr/base/misc.hpp"


namespace janus {
class SchedulerRococo;
class WaitlistChecker : public FrequentJob {
 public:
  SchedulerRococo *sched_ = nullptr;

  WaitlistChecker() = delete;
  WaitlistChecker(SchedulerRococo* sched);

  void Work() override;

};

} // namespace janus
