#ifndef WORKER_H_
#define WORKER_H_

#include <commons/string.h>

#include <utils/hello.h>
#include <utils/serializacion.h>
#include "worker-configs.h"
#include "worker-log.h"
#include <worker_interpreter.h>
#include "worker_memoria.h"

typedef struct {
    t_paquete* paquete;
    int socket_master;
    int socket_storage;
} t_paquete_y_sockets;


void* atender_master (void* arg);

#endif