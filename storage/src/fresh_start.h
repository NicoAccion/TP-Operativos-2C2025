#ifndef STORAGE_FRESH_START_H
#define STORAGE_FRESH_START_H

#include <commons/string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>

#include <utils/configs.h>
#include "storage-configs.h"
#include "fs_simulacion.h"

/**
 * @brief Inicializa el FS en base a la config del archivo superblock.config
 * 
 * Esta función configura el file system con los valores de las configs de storage y superblock
 */
void inicializar_fs();   


void inicializar_initial_file();


void crear_blocks_fisicos();

/**
 * @brief Crea un bloque lógico como un hard link a un bloque físico.
 * @param path_tag Directorio del tag donde se creará la carpeta logical_blocks.
 * Ej: "files/initial_file/BASE"
 * @param nro_bloque_fisico El número del bloque físico al cual vincular (ej: 0).
 * @param nro_bloque_logico El nombre del archivo de bloque lógico (ej: 0 -> 000000.dat).
 */
void crear_bloque_logico_como_link(const char* path_tag, int nro_bloque_fisico, int nro_bloque_logico);

/**
 * @brief Crea un directorio en la ruta especificada.
 *
 * @param dir_name El nombre del nuevo directorio a crear.
 *
 * Esta función construye la ruta completa 'punto_montaje/dir_name' e intenta crearla.
 * Si el directorio ya existe (error EEXIST), lo informa como una advertencia pero no
 * falla. Para cualquier otro error, se registra como un error crítico.
 */
void fs_crear_directorio(const char* dir_name);

#endif 