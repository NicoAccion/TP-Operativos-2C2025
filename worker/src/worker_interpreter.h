#ifndef WORKER_INTERPRETER_H
#define WORKER_INTERPRETER_H

#include <utils/serializacion.h>
#include <stdbool.h>
#include <worker-configs.h>
#include <worker-log.h>

extern uint32_t query_actual_id;
extern uint32_t query_actual_pc;
extern bool ejecutando_query;
extern bool desalojar_actual;
extern bool desconexion_actual;

void ejecutar_query(int query_id, char* path_query, uint32_t program_counter, int socket_master, int socket_storage);

#endif