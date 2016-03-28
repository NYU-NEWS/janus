
#include "../__dep__.h"
#include "../../rrr/base/misc.hpp"


namespace rococo {
class RccSched;
class WaitlistChecker : public FrequentJob {
 public:
  RccSched *sched_;

  WaitlistChecker() = delete;
  WaitlistChecker(RccSched* sched);

  void run() override;

};

} // namespace rococo