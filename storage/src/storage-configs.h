#ifndef STORAGE_CONFIGS_H
#define STORAGE_CONFIGS_H

#include <commons/string.h>

#include <utils/configs.h>
#include <utils/sockets.h>
#include <utils/hello.h>

extern t_config* storage_tconfig;

extern t_config* superblock_tconfig;

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                Funciones de las configs de storage

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
 * @struct storageconfigs
 * @brief Estructura que contiene la configuración del storage
 * 
 * @param puertoescucha
 * @param freshstart
 * @param puntomontaje
 * @param retardomontaje
 * @param retardooperacion
 * @param retardoaccesobloque
 * @param loglevel
 * 
 * Esta estructura almacena la configuración necesaria para el
 * funcionamiento del storage
 */
typedef struct storageconfigs {
    int puertoescucha;
    bool freshstart;
    char* puntomontaje;
    int retardooperacion;
    int retardoaccesobloque;
    char* loglevel;
} storageconfigs;

/**
 * @var storage_configs
 * @brief Estructura global de configuración del storage
 * 
 * Esta variable global permite acceder a la configuración del storage
 * desde cualquier archivo del módulo.
 */
extern storageconfigs storage_configs;

/**
 * @brief Inicializa la configuración del storage
 * 
 * Esta función carga la configuración del storage desde un archivo
 * de configuración y la almacena en la estructura global
 * `storage_configs`.
 */
int inicializar_configs();   

/**
 * @brief Destruye el struct storageconfigs
 * 
 * Esta función no recibe parámetros. Destruye el struct storageconfigs y libera la memoria
 * 
 */
void destruir_configs();

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                        Funciones de las configs del archivo superblock.configs

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
 * @struct superblockconfigs
 * @brief Estructura que contiene la configuración del archivo superblock.config
 * 
 * @param fssize
 * @param blocksize
 */

typedef struct superblockconfigs {
    int fssize;
    int blocksize;
} superblockconfigs;

/**
 * @var superblock_configs
 * @brief Estructura global de configuración del archivo superblock.config
 * 
 * Esta variable global permite acceder a la configuración del archivo superblock.config
 * desde cualquier archivo del módulo.
 */
extern superblockconfigs superblock_configs;

/**
 * @brief Inicializa la configuración del archivo superblock.config
 * 
 * Esta función carga la configuración del archivo superblock.config
 * y la almacena en la estructura global `superblock_configs`.
 */
int inicializar_superblock_configs();   

/**
 * @brief Destruye el struct superblockconfigs
 * 
 * Esta función no recibe parámetros. Destruye el struct superblockconfigs y libera la memoria
 * 
 */
void destruir_superblock_configs();

#endif