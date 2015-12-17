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
        LEARN = 0x5b3732d4,
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
        START = 0x5afa7258,
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
    virtual void Start(const StartRequest& r);
private:
    void __Start__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            StartRequest in_0;
            req->m >> in_0;
            this->Start(in_0);
            sconn->begin_reply(req);
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
    rrr::Future* async_Start(const StartRequest& r, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccClientService::START, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << r;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Start(const StartRequest& r) {
        rrr::Future* __fu__ = this->async_Start(r);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

class MdccLeaderService: public rrr::Service {
public:
    enum {
        PROPOSE = 0x118a56a0,
        RECOVER = 0x1db559ea,
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
    virtual void Propose(const ProposeRequest& r);
    virtual void Recover();
private:
    void __Propose__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            ProposeRequest in_0;
            req->m >> in_0;
            this->Propose(in_0);
            sconn->begin_reply(req);
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
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
    rrr::Future* async_Propose(const ProposeRequest& r, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccLeaderService::PROPOSE, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << r;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Propose(const ProposeRequest& r) {
        rrr::Future* __fu__ = this->async_Propose(r);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
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
        PROPOSE = 0x3572e2ae,
        PROPOSEFAST = 0x6d4ed8ba,
        ACCEPT = 0x4e870569,
        DECIDE = 0x21d1c287,
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
    virtual void Propose(const ProposeRequest& r);
    virtual void ProposeFast(const ProposeRequest& r);
    virtual void Accept(const AcceptRequest& r);
    virtual void Decide(const Result& r);
private:
    void __Propose__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            ProposeRequest in_0;
            req->m >> in_0;
            this->Propose(in_0);
            sconn->begin_reply(req);
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
    void __ProposeFast__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            ProposeRequest in_0;
            req->m >> in_0;
            this->ProposeFast(in_0);
            sconn->begin_reply(req);
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
    void __Accept__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            AcceptRequest in_0;
            req->m >> in_0;
            this->Accept(in_0);
            sconn->begin_reply(req);
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
    void __Decide__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        auto f = [=] {
            Result in_0;
            req->m >> in_0;
            this->Decide(in_0);
            sconn->begin_reply(req);
            sconn->end_reply();
            delete req;
            sconn->release();
        };
        sconn->run_async(f);
    }
};

class MdccAcceptorProxy {
protected:
    rrr::Client* __cl__;
public:
    MdccAcceptorProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_Propose(const ProposeRequest& r, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccAcceptorService::PROPOSE, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << r;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Propose(const ProposeRequest& r) {
        rrr::Future* __fu__ = this->async_Propose(r);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_ProposeFast(const ProposeRequest& r, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccAcceptorService::PROPOSEFAST, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << r;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 ProposeFast(const ProposeRequest& r) {
        rrr::Future* __fu__ = this->async_ProposeFast(r);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_Accept(const AcceptRequest& r, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccAcceptorService::ACCEPT, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << r;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Accept(const AcceptRequest& r) {
        rrr::Future* __fu__ = this->async_Accept(r);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_Decide(const Result& r, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(MdccAcceptorService::DECIDE, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << r;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 Decide(const Result& r) {
        rrr::Future* __fu__ = this->async_Decide(r);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        __fu__->release();
        return __ret__;
    }
};

} // namespace mdcc
