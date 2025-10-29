#ifndef STORAGE_OPERACIONES_H
#define STORAGE_OPERACIONES_H

#include <utils/serializacion.h> // Para t_op_storage y t_codigo_operacion
#include <commons/string.h>
#include <commons/config.h>
#include <sys/stat.h> // Para mkdir
#include <stdio.h>
#include "storage-configs.h"     // Para superblock_configs y storage_configs
#include "storage-log.h"         // Para el logger_storage

/**
 * @brief Ejecuta la lógica de creación de un File:Tag.
 * Valida si existe, crea directorios y metadata.config.
 * @param op Estructura con query_id, nombre_file y nombre_tag.
 * @return OP_OK si fue exitoso, OP_ERROR si falló (ej: ya existía).
 */
t_codigo_operacion storage_op_create(t_op_storage* op);

/**
 * @brief Ejecuta la lógica de truncado de un File:Tag.
 * (Próximo paso a implementar)
 * @param op Estructura con query_id, nombre_file, nombre_tag y tamano.
 * @return OP_OK si fue exitoso, OP_ERROR si falló.
 */
t_codigo_operacion storage_op_truncate(t_op_storage* op);

/**
 * @brief Ejecuta la lógica de eliminación de un File:Tag.
 * (Próximo paso a implementar)
 * @param op Estructura con query_id, nombre_file y nombre_tag.
 * @return OP_OK si fue exitoso, OP_ERROR si falló.
 */
t_codigo_operacion storage_op_delete(t_op_storage* op);

/**
 * @brief Ejecuta la lógica de commit de un File:Tag.
 * (Próximo paso a implementar)
 * @param op Estructura con query_id, nombre_file y nombre_tag.
 * @return OP_OK si fue exitoso, OP_ERROR si falló.
 */
t_codigo_operacion storage_op_commit(t_op_storage* op);

/**
 * @brief Ejecuta la lógica de creación de un nuevo Tag desde uno existente.
 * (Próximo paso a implementar)
 * @param op Estructura con file/tag origen y file/tag destino.
 * @return OP_OK si fue exitoso, OP_ERROR si falló.
 */
t_codigo_operacion storage_op_tag(t_op_storage* op);

// ... (Aquí irían las de READ y WRITE) ...

#endif