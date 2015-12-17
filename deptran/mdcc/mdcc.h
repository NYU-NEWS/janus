#pragma once

#include "rrr.hpp"

#include <errno.h>


namespace mdcc {

struct ConsensusValue {
};

inline rrr::Marshal& operator <<(rrr::Marshal& m, const ConsensusValue& o) {
    return m;
}

inline rrr::Marshal& operator >>(rrr::Marshal& m, ConsensusValue& o) {
    return m;
}

struct ProposalNum {
    rrr::i64 num;
    rrr::i64 server;
};

inline rrr::Marshal& operator <<(rrr::Marshal& m, const ProposalNum& o) {
    m << o.num;
    m << o.server;
    return m;
}

inline rrr::Marshal& operator >>(rrr::Marshal& m, ProposalNum& o) {
    m >> o.num;
    m >> o.server;
    return m;
}

struct StartRequest {
    rrr::i64 tid;
};

inline rrr::Marshal& operator <<(rrr::Marshal& m, const StartRequest& o) {
    m << o.tid;
    return m;
}

inline rrr::Marshal& operator >>(rrr::Marshal& m, StartRequest& o) {
    m >> o.tid;
    return m;
}

struct StartResponse {
    rrr::i8 result;
};

inline rrr::Marshal& operator <<(rrr::Marshal& m, const StartResponse& o) {
    m << o.result;
    return m;
}

inline rrr::Marshal& operator >>(rrr::Marshal& m, StartResponse& o) {
    m >> o.result;
    return m;
}

struct ProposeRequest {
    rrr::i64 start;
    rrr::i64 end;
    ProposalNum m;
};

inline rrr::Marshal& operator <<(rrr::Marshal& m, const ProposeRequest& o) {
    m << o.start;
    m << o.end;
    m << o.m;
    return m;
}

inline rrr::Marshal& operator >>(rrr::Marshal& m, ProposeRequest& o) {
    m >> o.start;
    m >> o.end;
    m >> o.m;
    return m;
}

struct ProposeResponse {
    rrr::i64 start;
    rrr::i64 end;
    ProposalNum m;
};

inline rrr::Marshal& operator <<(rrr::Marshal& m, const ProposeResponse& o) {
    m << o.start;
    m << o.end;
    m << o.m;
    return m;
}

inline rrr::Marshal& operator >>(rrr::Marshal& m, ProposeResponse& o) {
    m >> o.start;
    m >> o.end;
    m >> o.m;
    return m;
}

struct AcceptRequest {
    ProposalNum m;
    ConsensusValue v;
};

inline rrr::Marshal& operator <<(rrr::Marshal& m, const AcceptRequest& o) {
    m << o.m;
    m << o.v;
    return m;
}

inline rrr::Marshal& operator >>(rrr::Marshal& m, AcceptRequest& o) {
    m >> o.m;
    m >> o.v;
    return m;
}

struct AcceptResponse {
    ProposalNum m;
    ConsensusValue v;
};

inline rrr::Marshal& operator <<(rrr::Marshal& m, const AcceptResponse& o) {
    m << o.m;
    m << o.v;
    return m;
}

inline rrr::Marshal& operator >>(rrr::Marshal& m, AcceptResponse& o) {
    m >> o.m;
    m >> o.v;
    return m;
}

struct Result {
    rrr::i8 result;
    ConsensusValue v;
};

inline rrr::Marshal& operator <<(rrr::Marshal& m, const Result& o) {
    m << o.result;
    m << o.v;
    return m;
}

inline rrr::Marshal& operator >>(rrr::Marshal& m, Result& o) {
    m >> o.result;
    m >> o.v;
    return m;
}

class MdccLearnerService: public rrr::Service {
public:
    enum {
        LEARN = 0x199f3850,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(LEARN, this, &MdccLearnerService::__Learn__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(LEARN);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void Learn(const Result& r);
private:
    void __Learn__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            Result in_0;
            req->m >> in_0;
            this->Learn(in_0);
            sconn->begin_reply(req);
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
};

class MdccLearnerProxy {
protected:
    rrr::Client* __cl__;
public:
    MdccLearnerProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_Learn(const Result& r, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccLearnerService::LEARN, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << r;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Learn(const Result& r) {
        rrr::Future* __fu__ = this->async_Learn(r);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

class MdccClientService: public rrr::Service {
public:
    enum {
        START = 0x51934ab9,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(START, this, &MdccClientService::__Start__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(START);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void Start(const StartRequest& req, StartResponse* res);
private:
    void __Start__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            StartRequest in_0;
            req->m >> in_0;
            StartResponse out_0;
            this->Start(in_0, &out_0);
            sconn->begin_reply(req);
            *sconn << out_0;
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
};

class MdccClientProxy {
protected:
    rrr::Client* __cl__;
public:
    MdccClientProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_Start(const StartRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccClientService::START, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Start(const StartRequest& req, StartResponse* res) {
        rrr::Future* __fu__ = this->async_Start(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *res;
        }
        __fu__->release();
        return __ret__;
    }
};

class MdccLeaderService: public rrr::Service {
public:
    enum {
        PROPOSE = 0x2c365417,
        RECOVER = 0x415300c9,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(PROPOSE, this, &MdccLeaderService::__Propose__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(RECOVER, this, &MdccLeaderService::__Recover__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(PROPOSE);
        svr->unreg(RECOVER);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void Propose(const ProposeRequest& req, ProposeResponse* res, rrr::DeferredReply* defer);
    virtual void Recover();
private:
    void __Propose__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        ProposeRequest* in_0 = new ProposeRequest;
        req->m >> *in_0;
        ProposeResponse* out_0 = new ProposeResponse;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->Propose(*in_0, out_0, __defer__);
    }
    void __Recover__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            this->Recover();
            sconn->begin_reply(req);
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
};

class MdccLeaderProxy {
protected:
    rrr::Client* __cl__;
public:
    MdccLeaderProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_Propose(const ProposeRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccLeaderService::PROPOSE, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Propose(const ProposeRequest& req, ProposeResponse* res) {
        rrr::Future* __fu__ = this->async_Propose(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *res;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_Recover(const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccLeaderService::RECOVER, __fu_attr__);
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Recover() {
        rrr::Future* __fu__ = this->async_Recover();
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

class MdccAcceptorService: public rrr::Service {
public:
    enum {
        PROPOSE = 0x518662fc,
        PROPOSEFAST = 0x155cef39,
        ACCEPT = 0x51d40724,
        DECIDE = 0x20eebf07,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(PROPOSE, this, &MdccAcceptorService::__Propose__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(PROPOSEFAST, this, &MdccAcceptorService::__ProposeFast__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(ACCEPT, this, &MdccAcceptorService::__Accept__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(DECIDE, this, &MdccAcceptorService::__Decide__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(PROPOSE);
        svr->unreg(PROPOSEFAST);
        svr->unreg(ACCEPT);
        svr->unreg(DECIDE);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void Propose(const ProposeRequest& req, ProposeResponse* res, rrr::DeferredReply* defer);
    virtual void ProposeFast(const ProposeRequest& req, ProposeResponse* res, rrr::DeferredReply* defer);
    virtual void Accept(const AcceptRequest& req, AcceptResponse* res, rrr::DeferredReply* defer);
    virtual void Decide(const Result& result, rrr::DeferredReply* defer);
private:
    void __Propose__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        ProposeRequest* in_0 = new ProposeRequest;
        req->m >> *in_0;
        ProposeResponse* out_0 = new ProposeResponse;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->Propose(*in_0, out_0, __defer__);
    }
    void __ProposeFast__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        ProposeRequest* in_0 = new ProposeRequest;
        req->m >> *in_0;
        ProposeResponse* out_0 = new ProposeResponse;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->ProposeFast(*in_0, out_0, __defer__);
    }
    void __Accept__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        AcceptRequest* in_0 = new AcceptRequest;
        req->m >> *in_0;
        AcceptResponse* out_0 = new AcceptResponse;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->Accept(*in_0, out_0, __defer__);
    }
    void __Decide__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        Result* in_0 = new Result;
        req->m >> *in_0;
        auto __marshal_reply__ = [=] {
        };
        auto __cleanup__ = [=] {
            delete in_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->Decide(*in_0, __defer__);
    }
};

class MdccAcceptorProxy {
protected:
    rrr::Client* __cl__;
public:
    MdccAcceptorProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_Propose(const ProposeRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccAcceptorService::PROPOSE, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Propose(const ProposeRequest& req, ProposeResponse* res) {
        rrr::Future* __fu__ = this->async_Propose(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *res;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_ProposeFast(const ProposeRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccAcceptorService::PROPOSEFAST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 ProposeFast(const ProposeRequest& req, ProposeResponse* res) {
        rrr::Future* __fu__ = this->async_ProposeFast(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *res;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_Accept(const AcceptRequest& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccAcceptorService::ACCEPT, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Accept(const AcceptRequest& req, AcceptResponse* res) {
        rrr::Future* __fu__ = this->async_Accept(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *res;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_Decide(const Result& result, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccAcceptorService::DECIDE, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << result;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Decide(const Result& result) {
        rrr::Future* __fu__ = this->async_Decide(result);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

} // namespace mdcc



