#ifndef ATENDER_HILOS_H_
#define ATENDER_HILOS_H_

#include <commons/collections/list.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdbool.h>
#include <string.h>

#include "master-configs.h"
#include "master-log.h"
#include <utils/serializacion.h>

// Estructura para la info de un worker conectado
typedef struct {
    int socket_cliente;
    uint32_t id_worker;
    bool libre;
    t_query_completa* query_asignada; // Puntero a la query que está ejecutando
} t_worker_completo;


// Variables globales para las listas y contadores
extern t_list* ready;
extern t_list* exec;
extern t_list* workers;

// Variables globales para sincronización
extern pthread_mutex_t mutex_ready;
extern pthread_mutex_t mutex_exec;
extern pthread_mutex_t mutex_workers;
extern sem_t sem_queries_en_ready;


// --- Prototipos de Funciones ---
void inicializar_estructuras_globales();
void destruir_estructuras_globales();

void manejar_nueva_conexion(void* arg);
void atender_query_control(int socket_cliente, t_paquete* paquete);
void atender_worker(int socket_cliente, t_paquete* paquete);

#endif