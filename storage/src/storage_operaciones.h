#ifndef STORAGE_OPERACIONES_H
#define STORAGE_OPERACIONES_H

#include <utils/serializacion.h> // Para t_op_storage y t_codigo_operacion
#include <commons/string.h>
#include <commons/config.h>
#include <commons/crypto.h>
#include <sys/stat.h> // Para mkdir
#include <stdio.h>
#include "storage-configs.h"     // Para superblock_configs y storage_configs
#include "storage-log.h"         // Para el logger_storage
#include <dirent.h> // Para readdir/opendir (necesario para borrar)
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <string.h>


/**
 * @brief Ejecuta la lógica de creación de un File:Tag.
 * Valida si existe, crea directorios y metadata.config.
 * @param op Estructura con query_id, nombre_file y nombre_tag.
 * @return OP_OK si fue exitoso, OP_ERROR si falló 
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

//fucniones aux
char* build_blocks_string(char** bloques_actuales, int count_actual, int count_nuevo);
void chequear_y_liberar_bloque_fisico(int query_id, char* nro_bloque_fisico_str);
char* array_to_blocks_string(char** bloques_array, int count); //seria como un string_join

void escribir_en_bloque_fisico(char* path_bloque_fisico, void* contenido, int tamano_contenido, int block_size);
int encontrar_bloque_libre_mock(int query_id);
int reservar_bloque_real(int query_id);
// ... (Aquí irían las de READ y WRITE) ...

t_codigo_operacion storage_op_write(t_op_storage* op);
// Esta función devuelve el contenido leído por un "out-parameter" (char**)
t_codigo_operacion storage_op_read(t_op_storage* op, char** contenido_leido);

#endif