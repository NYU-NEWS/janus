#pragma once

void mlock_init();

void m_lock(char *name);

void m_unlock(char *name);

pthread_mutex_t* m_getlock(char* name);
