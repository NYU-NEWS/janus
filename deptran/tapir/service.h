#pragma once

#include "../command.h"
#include "../command_marshaler.h"
#include "tapir_srpc.h"

namespace rococo {

class Scheduler;
class TapirSched;
class TapirServiceImpl: public TapirService {
 public:
  TapirSched* sched_;
  TapirServiceImpl(Scheduler* sched);
  void Prepare(rrr::DeferredReply* defer) override;
  void Accept(rrr::DeferredReply* defer) override;
  void FastAccept(const SimpleCommand& cmd,
                  rrr::i32* res,
                  std::map<rrr::i32, Value>* output,
                  rrr::DeferredReply* defer) override;
  void Decide(const rrr::i64& cmd_id,
              const rrr::i32& commit,
              rrr::DeferredReply* defer) override;
};

} // namespace rococo