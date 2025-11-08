#ifndef WORKER_INTERPRETER_H
#define WORKER_INTERPRETER_H

#include <utils/serializacion.h>
#include <stdbool.h>
#include <worker-configs.h>
#include <worker-log.h>


void ejecutar_query(int query_id, const char* path_query, uint32_t program_counter, int socket_master, int socket_storage);

#endif