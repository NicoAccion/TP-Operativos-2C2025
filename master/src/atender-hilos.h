#ifndef ATENDER_HILOS_H_
#define ATENDER_HILOS_H_

#include "master-configs.h"
#include "master-log.h"
#include <utils/serializacion.h>

#include <commons/collections/list.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <string.h>

/*//////////////////////////////////////////////////////////////////////////////////////////////////

                                        Variables globales

//////////////////////////////////////////////////////////////////////////////////////////////////*/

// Variables globales para las listas y contadores
extern t_list* ready;
extern t_list* exec;
extern t_list* workers;

// Variables globales para sincronización
extern pthread_mutex_t mutex_ready;
extern pthread_mutex_t mutex_exec;
extern pthread_mutex_t mutex_workers;
extern sem_t sem_queries_en_ready;
extern sem_t sem_workers_libres;
extern sem_t sem_planificar_prioridad;

/*//////////////////////////////////////////////////////////////////////////////////////////////////

                                        Estructuras

//////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
 * @struct t_query_completa
 * @brief Estructura que representa una query completa.
 *  * 
 * @param socket_cliente: Socket del query control conectado
 * @param archivo_query: Archivo que contiene la query a ejecutar
 * @param prioridad: Prioridad de la query
 * @param id_query: Identificador de la query
 * @param estado: Estado en el que se encuentra la query
 * @param program_counter: Program Counter de la query
 */
typedef struct {
    uint32_t socket_cliente;
    char* archivo_query;
    uint32_t prioridad;
    uint32_t id_query;
    t_estado estado;
    uint32_t program_counter; 
} t_query_completa;

/**
 * @struct t_worker_completo
 * @brief Estructura que representa un worker
 *  * 
 * @param socket_cliente: Socket del worker conectado
 * @param id_worker: Identificador del worker
 * @param libre: Bool que indica si el worker se encuentra libre
 * @param query_asignada: query que fue asignada al worker
 */
typedef struct {
    int socket_cliente;
    uint32_t id_worker;
    bool libre;
    t_query_completa* query_asignada; // Puntero a la query que está ejecutando
} t_worker_completo;

/*//////////////////////////////////////////////////////////////////////////////////////////////////

                                        Prototipos de funciones

//////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
 * @brief Inicializa las estructuras globales
 * 
 * @return No devuelve nada
 */
void inicializar_estructuras_globales();

/**
 * @brief Libera la memoria de las estructuras globales
 * 
 * @return No devuelve nada
 */
void destruir_estructuras_globales();

/**
 * @brief Hilo encargado de manejar las conexiones con el master
 * 
 * @param arg: Puntero al socket de la conexión
 * @return No devuelve nada
 */
void manejar_nueva_conexion(void* arg);

/**
 * @brief Se encarga de atender a un nuevo query control
 * 
 * @param socket_cliente: Socket de la conexión con el query conytol
 * @param paquete: Paquete que recibió del query control
 * 
 * @return No devuelve nada
 */
void atender_query_control(int socket_cliente, t_paquete* paquete);

/**
 * @brief Se encarga de atender a un nuevo worker
 * 
 * @param socket_cliente: Socket de la conexión con el worker
 * @param paquete: Paquete que recibió del worker
 * @return No devuelve nada
 */
void atender_worker(int socket_cliente, t_paquete* paquete);

/**
 * @brief Busca el worker donde fue asignada la query
 * 
 * @param query: La query que queremos buscar a que worker fue asignada
 * @return t_worker_completo* Puntero al worker donde fue asignada la query
 */
 t_worker_completo* buscar_worker_asignado(t_query_completa* query);

/**
 * @brief Finaliza una query y libera su memoria
 * 
 * @param query: La query que queremos finalizar
 * @return No devuelve nada
 */
void finalizar_query(t_query_completa* query);

#endif