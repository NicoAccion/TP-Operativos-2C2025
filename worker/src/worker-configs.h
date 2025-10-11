#ifndef WORKER_CONFIGS_H
#define WORKER_CONFIGS_H

#include <utils/configs.h>
#include <utils/sockets.h>
#include <utils/hello.h>

extern t_config* worker_tconfig;

/**
 * @struct workerconfigs
 * @brief Estructura que contiene la configuración del worker
 * 
 * @param ipmaster
 * @param puertomaster
 * @param ipstorage
 * @param puertostorage
 * @param tammemoria
 * @param retardomemoria
 * @param algoritmoreemplazo
 * @param pathqueries
 * @param loglevel
 * 
 * Esta estructura almacena la configuración necesaria para el
 * funcionamiento del worker
 */
typedef struct workerconfigs {
    char* ipmaster;
    int puertomaster;
    char* ipstorage;
    int puertostorage;
    int tammemoria;
    int retardomemoria;
    char* algoritmoreemplazo;
    char* pathqueries;
    char* loglevel;
} workerconfigs;

/**
 * @var worker_configs
 * @brief Estructura global de configuración del worker
 * 
 * Esta variable global permite acceder a la configuración del worker
 * desde cualquier archivo del módulo.
 */
extern workerconfigs worker_configs;

/**
 * @brief Inicializa la configuración del worker
 * @param path_config el path del archivo
 * 
 * Esta función carga la configuración del worker desde un archivo
 * de configuración y la almacena en la estructura global
 * `worker_configs`.
 */
int inicializar_configs(char* path_config);   

/**
 * @brief Destruye el struct workerconfigs
 * 
 * Esta función no recibe parámetros. Destruye el struct workerconfigs y libera la memoria
 * 
 */
void destruir_configs();

#endif