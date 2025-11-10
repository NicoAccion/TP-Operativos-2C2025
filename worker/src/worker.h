#ifndef WORKER_H_
#define WORKER_H_

#include <commons/string.h>

#include <utils/hello.h>
#include <utils/serializacion.h>
#include "worker-configs.h"
#include "worker-log.h"
#include <worker_interpreter.h>
#include "worker_memoria.h"

/**
 * @brief Envía una operación simple (CREATE, WRITE, TRUNCATE, etc.)
 * y espera una respuesta OK/ERROR.
 */
t_codigo_operacion enviar_op_simple_storage(int socket_storage, t_codigo_operacion op_code, t_op_storage* op);

/**
 * @brief Envía una operación READ al Storage y espera un paquete READ_RTA
 * con el contenido.
 * @return El contenido leído (char*), o NULL si falló.
 */
char* enviar_op_read_storage(int socket_storage, t_op_storage* op);


#endif