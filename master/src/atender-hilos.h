#ifndef ATENDER_HILOS_H_
#define ATENDER_HILOS_H_

#include <commons/collections/list.h>

#include "master-configs.h"
#include "master-log.h"

#include <utils/serializacion.h>

#include <pthread.h>

typedef struct {
    t_paquete* paquete;
    int cliente;
} t_conexion;

typedef struct {
    uint32_t socket_cliente;
    uint32_t id_worker;
    bool libre;
    t_query_completa query;
} t_worker_completo;

extern uint32_t contador_queries;

extern uint32_t contador_workers;

extern t_list* ready;

extern t_list* exec;

extern t_list* workers;

void* atender_query_control(void* arg);

void* atender_worker(void* arg);

void* escuchar_worker(void* arg);

bool buscar_id_query(void* arg);

bool buscar_id_worker(void* arg);

#endif