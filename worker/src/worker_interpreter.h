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
extern pthread_mutex_t mutex_flags;

void inicializar_estructuras_globales();

void destruir_estructuras_globales();

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

void ejecutar_query(int query_id, char* path_query, uint32_t program_counter, int socket_master, int socket_storage);

#endif