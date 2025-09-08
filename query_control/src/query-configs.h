#ifndef QUERY_CONFIGS_H
#define QUERY_CONFIGS_H

#include <utils/configs.h>
#include <utils/sockets.h>
#include <utils/hello.h>

extern t_config* query_tconfig;

/**
 * @struct queryconfigs
 * @brief Estructura que contiene la configuración del query
 * 
 * @param ipmaster: IP del módulo Master
 * @param puertomaster: Puerto del módulo Master
 * @param loglevel: Nivel de detalle máximo a mostrar en query
 * 
 * Esta estructura almacena la configuración necesaria para el
 * funcionamiento del query
 */
typedef struct queryconfigs {
     char* ipmaster;
     int puertomaster;
     char* loglevel;
} queryconfigs;

/**
 * @var query_configs
 * @brief Estructura global de configuración del query
 * 
 * Esta variable global permite acceder a la configuración del query
 * desde cualquier archivo del módulo.
 */
extern queryconfigs query_configs;

/**
 * @brief Inicializa la configuración del query
 * @param path_config el path del archivo
 * 
 * Esta función carga la configuración del query desde un archivo
 * de configuración y la almacena en la estructura global
 * `query_configs`.
 */
int inicializar_configs(char* path_config);   

/**
 * @brief Destruye el struct queryconfigs
 * 
 * Esta función no recibe parámetros. Destruye el struct queryconfigs y libera la memoria
 * 
 */
void destruir_configs();

#endif