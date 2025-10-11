#ifndef STORAGE_CONFIGS_H
#define STORAGE_CONFIGS_H

#include <sys/stat.h>
#include <commons/string.h>
#include <commons/collections/list.h>
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
int inicializar_configs(char* path);   

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

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                        Funciones de las configs del archivo metadata.config

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
 * @struct metadataconfigs
 * @brief Estructura que contiene la configuración de los archivos tipo metadata.config
 * 
 * @param tamanio
 * @param blocks
 * @param estado
 */
typedef struct metadataconfigs {
    int tamanio;
    t_list* blocks;
    char* estado;
} metadataconfigs;


/**
 * @brief Inicializa la configuración del archivo metadata.config
 * 
 * Esta función carga la configuración del archivo metadata.config
 */
metadataconfigs* inicializar_metadata_config();

/**
 * @brief Destruye el struct metadataconfigs
 * 
 * Destruye el struct metadataconfigs y libera la memoria
 * 
 */
void destruir_metadata_configs(metadataconfigs* metadata_config);


/**
 * @brief Convierte una t_list de números (uint32_t*) a un string como "[1,2,3]".
 * @param lista La lista de bloques.
 * @return Un nuevo string que debes liberar con free().
 */
char* convertir_lista_a_string(t_list* lista);

/**
 * @brief Guarda la información de un struct metadataconfigs en un archivo de configuración.
 * @param metadata Puntero al struct con la información.
 * @param path Ruta del archivo donde se guardará la configuración.
 */
void guardar_metadata_en_archivo(metadataconfigs* metadata, char* path);

#endif