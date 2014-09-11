#include <counter.h>
#include <pthread.h>
#include <stdlib.h>

using namespace rpc;

char **servers;
unsigned int ns;
unsigned int npg;
rrr::PollMgr **pm;

CounterProxy **get_proxy() {
    unsigned int i = 0;
    CounterProxy **ret = (CounterProxy **)malloc(sizeof(CounterProxy *) * ns);
    for (; i < ns; i++) {
        rrr::Client *client = new rrr::Client(pm[i]);
        client->connect(servers[i]);
        ret[i] = new CounterProxy(client);
    }
    return ret;
}


void *do_add(void *) {
    CounterProxy **proxy = get_proxy();
    unsigned int start = rand() % ns;
    unsigned int i = 0, j;
    while (1) {
        rrr::FutureGroup fg;
        for (i = 0; i < ns; i++) {
            for (j = 0; j < npg; j++) {
                fg.add(proxy[start++]->async_add());
                start %= ns;
            }
        }
        fg.wait_all();
    }
    return NULL;
}

void *do_add_long(void *) {
    CounterProxy **proxy = get_proxy();
    unsigned int start = rand() % ns;
    unsigned int i = 0;
    while (1) {
        rrr::FutureGroup fg;
        for (i = 0; i < npg; i++) {
            fg.add(proxy[start++]->async_add_long(1, 2, 3, 4, 5, std::vector<rrr::i64>(2, 1)));
            start %= ns;
        }
        fg.wait_all();
    }
    return NULL;
}

void *do_add_short(void *) {
    CounterProxy **proxy = get_proxy();
    unsigned int start = rand() % ns;
    unsigned int i = 0;
    while (1) {
        rrr::FutureGroup fg;
        for (i = 0; i < npg; i++) {
            fg.add(proxy[start++]->async_add_short((rrr::i64)1));
            start %= ns;
        }
        fg.wait_all();
    }
    return NULL;
}

int main(int argc, char **argv) {
    if (argc < 5)
        return -1;

    unsigned int nt = atoi(argv[1]);

    void *(*func)(void *);
    switch(atoi(argv[2])) {
        case 0:
            func = &do_add;
            break;
        case 1:
            func = &do_add_short;
            break;
        case 2:
            func = &do_add_long;
            break;
        default:
            return -3;
    }

    npg = atoi(argv[3]);
    ns = atoi(argv[4]);

    if ((unsigned int)argc < 5 + ns)
        return -2;

    servers = (char **)malloc(ns * sizeof(char *));
    pm = (rrr::PollMgr **)malloc(sizeof(rrr::PollMgr *) * ns);

    unsigned int i = 0;
    for (; i < ns; i++) {
        servers[i] = argv[i + 5];
        pm[i] = new rrr::PollMgr();
    }

    pthread_t *ph = (pthread_t *)malloc(sizeof(pthread_t) * nt);
    for (i = 0; i < nt; i++)
        pthread_create(ph + i, NULL, func, NULL);

    for (i = 0; i < nt; i++)
        pthread_join(ph[i], NULL);

    return 0;
}
