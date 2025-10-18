#ifndef WORKER_H_
#define WORKER_H_

#include <commons/string.h>

#include <utils/hello.h>
#include <utils/serializacion.h>
#include "worker-configs.h"
#include "worker-log.h"


void enviar_operacion_storage(int socket_storage, t_codigo_operacion op_code, t_operacion_query* op_query);

#endif