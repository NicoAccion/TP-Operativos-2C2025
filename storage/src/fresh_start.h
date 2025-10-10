
#include <commons/string.h>
#include <sys/stat.h>

#include <utils/configs.h>
#include "storage-configs.h"

/**
 * @brief Inicializa el FS en base a la config del archivo superblock.config
 * 
 * Esta función configura el file system con los valores de las configs de storage y superblock
 */
void inicializar_directorios();   


bool inicializar_initial_file();