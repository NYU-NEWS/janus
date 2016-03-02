#pragma once

#include "../command.h"
#include "../command_marshaler.h"
#include "tapir_srpc.h"

namespace rococo {

class Scheduler;
class TapirSched;
class TapirServiceImpl: public TapirService {
 public:
  TapirSched* sched_ = nullptr;
  TapirServiceImpl(Scheduler* sched);
  void Handout(const SimpleCommand& cmd,
               rrr::i32* res,
               map<int32_t, Value>* output,
               rrr::DeferredReply* reply) override;
  void Prepare(rrr::DeferredReply* defer) override;
  void Accept(const cmdid_t& cmd_id,
              const ballot_t& ballot,
              const int32_t& decision,
              rrr::DeferredReply* defer) override;
  void FastAccept(const SimpleCommand& cmd,
                  rrr::i32* res,
                  std::map<rrr::i32, Value>* output,
                  rrr::DeferredReply* defer) override;
  void Decide(const rrr::i64& cmd_id,
              const rrr::i32& decision,
              rrr::DeferredReply* defer) override;
};

} // namespace rococo