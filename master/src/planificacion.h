#ifndef PLANIFICACION_H
#define PLANIFICACION_H

#include "atender-hilos.h"

/*//////////////////////////////////////////////////////////////////////////////////////////////////

                                        Prototipos de funciones

//////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
 * @brief Hilo que decide con que algoritmo planificar
 * 
 * @return NULL
 */
void* planificar();

/**
 * @brief Planifica a las queries con el algoritmo FIFO
 * 
 * @return No devuelve nada
 */
void fifo();

/**
 * @brief Planifica a las queries con el algoritmo de prioridades
 * 
 * @return No devuelve nada
 */
void prioridades();

/**
 * @brief Asigna la query al primer worker libre
 * 
 * @param query: Query a asignar
 * @return No devuelve nada
 */
void asignar_query(t_query_completa* query);

/**
 * @brief Bool que indica si el worker está libre
 * 
 * @param arg: Puntero al worker
 * @return bool: Devuelve true si el worker está libre y false si está ocupado
 */
bool esta_libre(void* arg);

/**
 * @brief Compara cual de las dos queries tiene mayor prioridad
 * 
 * @param arg1: Primera query a comparar
 * @param arg2: Segunda query a comparar
 * @return void*: Puntero a la query con mayor prioridad
 */
void* mayor_prioridad(void* arg1, void* arg2);

/**
 * @brief Compara cual de las dos queries tiene menor prioridad
 * 
 * @param arg1: Primera query a comparar
 * @param arg2: Segunda query a comparar
 * @return void*: Puntero a la query con menor prioridad
 */
void* menor_prioridad(void* arg1, void* arg2);

/**
 * @brief Hilo que se encarga de aumentar la prioridad de las queries
 * 
 * @return No devuelve nada
 */
void* aging();

#endif