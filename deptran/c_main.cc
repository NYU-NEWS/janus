#include "all.h"

using namespace rococo;

typedef struct {
    unsigned int coo_id;
    std::vector<std::string> *servers;
    int benchmark;
    int mode;
    bool batch_start;
    unsigned int id;
    unsigned int duration;
    ClientControlServiceImpl *ccsi;
    unsigned int concurrent_txn;
} worker_attr_t;

void *coo_work(void *_attr) {
    worker_attr_t *attr = (worker_attr_t *)_attr;
    unsigned int id = attr->id;
    unsigned int duration = attr->duration;
    ClientControlServiceImpl *ccsi = attr->ccsi;
    unsigned int concurrent_txn = attr->concurrent_txn;
    uint32_t coo_id = (uint32_t)attr->coo_id;
    Coordinator *coo = new Coordinator(coo_id, *(attr->servers),
				       attr->benchmark, attr->mode,
				       ccsi, id, attr->batch_start);
    //pthread_mutex_t finish_mutex;
    rrr::Mutex finish_mutex;
    //pthread_cond_t finish_cond;
    rrr::CondVar finish_cond;
    //pthread_mutex_init(&finish_mutex, NULL);
    //pthread_cond_init(&finish_cond, NULL);
    if (ccsi != NULL) {
        std::function<void(TxnReply&)> callback = [coo_id, id, coo, ccsi,
						   &callback, &finish_mutex,
						   &finish_cond, &concurrent_txn] (TxnReply &txn_reply) {
            TIMER_IF_NOT_END {
                TxnRequest req;
                TxnRequestFactory::init_txn_req(&req, coo_id);
                req.callback_ = callback;
                coo->do_one(req);
            }
            else {
                Log::debug("time up. stop.");
                //pthread_mutex_lock(&finish_mutex);
                finish_mutex.lock();
                concurrent_txn--;
                if (concurrent_txn == 0)
                    //pthread_cond_signal(&finish_cond);
                    finish_cond.signal();
                //pthread_mutex_unlock(&finish_mutex);
                finish_mutex.unlock();
            }
        };

        ccsi->wait_for_start(id);
        TIMER_SET(duration);
        for (unsigned int n_txn = 0; n_txn < concurrent_txn; n_txn++) {
            TxnRequest req;
            TxnRequestFactory::init_txn_req(&req, coo_id);
            req.callback_ = callback;
            coo->do_one(req);
        }
        //pthread_mutex_lock(&finish_mutex);
        finish_mutex.lock();
        while (concurrent_txn > 0)
            //pthread_cond_wait(&finish_cond, &finish_mutex);
            finish_cond.wait(finish_mutex);
        //pthread_mutex_unlock(&finish_mutex);
        finish_mutex.unlock();
        ccsi->wait_for_shutdown();
    }
    else {
        std::atomic<unsigned int> num_txn, success, num_try;
        num_txn.store(0);
        success.store(0);
        num_try.store(0);
        std::function<void(TxnReply&)> callback = [coo_id, coo, &callback, &num_txn, &success, &num_try, &finish_mutex, &finish_cond, &concurrent_txn] (TxnReply &txn_reply) {
            TIMER_IF_NOT_END {
                TxnRequest req;
                TxnRequestFactory::init_txn_req(&req, coo_id);
                req.callback_ = callback;
                coo->do_one(req);
            }
            else {
                //pthread_mutex_lock(&finish_mutex);
                finish_mutex.lock();
                concurrent_txn--;
                if (concurrent_txn == 0)
                    //pthread_cond_signal(&finish_cond);
                    finish_cond.signal();
                //pthread_mutex_unlock(&finish_mutex);
                finish_mutex.unlock();
            }
            if (txn_reply.res_ == SUCCESS)
                success++;
            num_txn++;
            num_try.fetch_add(txn_reply.n_try_);
        };
        TIMER_SET(duration);
        //pthread_mutex_lock(&finish_mutex);
        finish_mutex.lock();
        for (unsigned int n_txn = 0; n_txn < concurrent_txn; n_txn++) {
            TxnRequest req;
            TxnRequestFactory::init_txn_req(&req, coo_id);
            req.callback_ = callback;
            coo->do_one(req);
        }
        while (concurrent_txn > 0)
            //pthread_cond_wait(&finish_cond, &finish_mutex);
            finish_cond.wait(finish_mutex);
        //pthread_mutex_unlock(&finish_mutex);
        finish_mutex.unlock();
        Log_info("Finish:\nTotal: %u, Commit: %u, Attempts: %u, Running for %u\n", num_txn.load(), success.load(), num_try.load(), Config::get_config()->get_duration());
    }

    delete coo;
    //pthread_mutex_destroy(&finish_mutex);
    //pthread_cond_destroy(&finish_cond);

    return NULL;
}

int main(int argc, char *argv[]) {
    int ret;

    if (0 != (ret = Config::create_config(argc, argv)))
        return ret;

    //unsigned int cid = Config::get_config()->get_client_id();

    bool hb = Config::get_config()->do_heart_beat();
    unsigned int num_threads = Config::get_config()->get_num_threads(); // func

    TxnRequestFactory::init_txn_req(NULL, 0);

    std::map<int32_t, std::string> txn_types;
    TxnRequestFactory::get_txn_types(txn_types);

    rrr::Server *hb_server = NULL;
    ClientControlServiceImpl *ccsi = NULL;
    if (hb) {
        // setup controller rpc server
        ccsi = new ClientControlServiceImpl(num_threads, txn_types);
        int n_io_threads = 1;
        rrr::PollMgr *poll_mgr = new rrr::PollMgr(n_io_threads);
        base::ThreadPool *thread_pool = new base::ThreadPool(1);
        hb_server = new rrr::Server(poll_mgr, thread_pool);
        hb_server->reg(ccsi);
        hb_server->start(std::string("0.0.0.0:").append(std::to_string(Config::get_config()->get_ctrl_port())).c_str());
    }

    unsigned int duration = Config::get_config()->get_duration();
    if (duration == 0)
        return -1;

    std::vector<std::string> servers;
    unsigned int num_site = Config::get_config()->get_all_site_addr(servers);
    if (num_site == 0)
        return -2;

    worker_attr_t *worker_attrs = (worker_attr_t *)malloc(sizeof(worker_attr_t) * num_threads);
    pthread_t *coo_threads = (pthread_t *)malloc(sizeof(pthread_t) * num_threads);

    unsigned int start_coo_id = Config::get_config()->get_start_coordinator_id(); // func

    int benchmark = Config::get_config()->get_benchmark();

    int mode = Config::get_config()->get_mode();

    unsigned int concurrent_txn = Config::get_config()->get_concurrent_txn();

    bool batch_start = Config::get_config()->get_batch_start();

    unsigned int thread_index = 0;
    for (; thread_index < num_threads; thread_index++) {

        worker_attrs[thread_index].servers = &servers;
        worker_attrs[thread_index].coo_id = start_coo_id + thread_index;
        worker_attrs[thread_index].benchmark = benchmark;
        worker_attrs[thread_index].mode = mode;
        worker_attrs[thread_index].batch_start = batch_start;
        worker_attrs[thread_index].id = thread_index;
        worker_attrs[thread_index].duration = duration;
        worker_attrs[thread_index].ccsi = ccsi;
        worker_attrs[thread_index].concurrent_txn = concurrent_txn;

        pthread_create(coo_threads + thread_index, NULL, &coo_work, worker_attrs + thread_index);
    }

    for (thread_index = 0; thread_index < num_threads; thread_index++)
        pthread_join(coo_threads[thread_index], NULL);

    free(worker_attrs);
    free(coo_threads);

    TxnRequestFactory::destroy();
    RandomGenerator::destroy();
    Config::destroy_config();
    return 0;
}
