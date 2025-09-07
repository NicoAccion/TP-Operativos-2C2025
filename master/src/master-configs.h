#ifndef MASTER_CONFIGS_H
#define MASTER_CONFIGS_H

#include <utils/configs.h>
#include <utils/sockets.h>
#include <utils/hello.h>

extern t_config* master_tconfig;

/**
 * @struct masterconfigs
 * @brief Estructura que contiene la configuración del master
 * 
 * @param puertoescucha: Puerto del módulo master
 * @param algoritmoplanificacion: Algoritmo de planificacion FIFO/prioridades
 * @param tiempoaging: Aging de prioridades
 * @param loglevel: Nivel de detalle máximo a mostrar en master
 * 
 * Esta estructura almacena la configuración necesaria para el
 * funcionamiento del master, incluyendo direcciones IP, puertos y
 * parámetros de reemplazo de caché y TLB.
 */
typedef struct masterconfigs {
     int puertoescucha;
     char* algoritmoplanificacion;
     int tiempoaging;
     char* loglevel;
} masterconfigs;

/**
 * @var master_configs
 * @brief Estructura global de configuración del master
 * 
 * Esta variable global permite acceder a la configuración del master
 * desde cualquier archivo del módulo.
 */
extern masterconfigs master_configs;

/**
 * @brief Inicializa la configuración del master
 * 
 * Esta función carga la configuración del master desde un archivo
 * de configuración y la almacena en la estructura global
 * `master_configs`.
 */
int inicializar_configs();   

/**
 * @brief Destruye el struct masterconfigs
 * 
 * Esta función no recibe parámetros. Destruye el struct masterconfigs y libera la memoria
 * 
 */
void destruir_configs();

#endif