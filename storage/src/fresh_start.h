#ifndef STORAGE_FRESH_START_H
#define STORAGE_FRESH_START_H

#include <commons/string.h>
#include <sys/stat.h>

#include <utils/configs.h>
#include "storage-configs.h"
#include "fs_simulacion.h"

/**
 * @brief Inicializa el FS en base a la config del archivo superblock.config
 * 
 * Esta función configura el file system con los valores de las configs de storage y superblock
 */
void inicializar_directorios();   


void inicializar_initial_file();


void crear_blocks_fisicos();
#endif 