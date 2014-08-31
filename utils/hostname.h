
#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>

static char* gethostip(const char* hostname) {
    struct hostent *h = NULL;
    char *buf = (char*)malloc(100);
    h = gethostbyname(hostname);
    if (h == NULL) {
        return NULL;
    } else {
        inet_ntop(h->h_addrtype, *h->h_addr_list, buf, 100);
        return buf;
    }
}
