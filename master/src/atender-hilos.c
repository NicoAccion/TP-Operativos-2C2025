#include "atender-hilos.h"

// --- Variables Globales ---
uint32_t contador_queries = 0;

t_list* ready;
t_list* exec;
t_list* workers;

// --- Sincronización ---
pthread_mutex_t mutex_ready;
pthread_mutex_t mutex_exec;
pthread_mutex_t mutex_workers;
sem_t sem_queries_en_ready;

void inicializar_estructuras_globales() {
    ready = list_create();
    exec = list_create();
    workers = list_create();

    pthread_mutex_init(&mutex_ready, NULL);
    pthread_mutex_init(&mutex_exec, NULL);
    pthread_mutex_init(&mutex_workers, NULL);
    sem_init(&sem_queries_en_ready, 0, 0); // Inicia en 0
}

void destruir_estructuras_globales() {
    
    list_destroy_and_destroy_elements(ready, free);
    list_destroy_and_destroy_elements(exec, free);
    list_destroy_and_destroy_elements(workers, free);

    pthread_mutex_destroy(&mutex_ready);
    pthread_mutex_destroy(&mutex_exec);
    pthread_mutex_destroy(&mutex_workers);
    sem_destroy(&sem_queries_en_ready);
}

void manejar_nueva_conexion(void* arg) {
    int socket_cliente = *((int*) arg);
    free(arg);

    t_paquete* paquete = recibir_paquete(socket_cliente);
    if (paquete == NULL) {
        log_error(logger_master, "No se pudo recibir el handshake del cliente en socket %d", socket_cliente);
        close(socket_cliente);
        return;
    }

    switch(paquete->codigo_operacion) {
        case HANDSHAKE_QUERYCONTROL:
            atender_query_control(socket_cliente, paquete);
            break;
        case HANDSHAKE_WORKER:
            atender_worker(socket_cliente, paquete);
            break;
        default:
            log_warning(logger_master, "Handshake desconocido recibido. Desconectando cliente.");
            liberar_paquete(paquete);
            close(socket_cliente);
            break;
    }
}

void atender_query_control(int socket_cliente, t_paquete* paquete) {
    // 1. Deserializar la query usando el paquete recibido
    t_query* query = deserializar_query(paquete->buffer);
    liberar_paquete(paquete);

    // 2. Crear la estructura interna de la query
    t_query_completa* query_completa = malloc(sizeof(t_query_completa));
    query_completa->socket_cliente = socket_cliente;
    query_completa->archivo_query = strdup(query->archivo_query); // Usar strdup para copiar el string
    query_completa->prioridad = query->prioridad;
    query_completa->estado = READY;

    // 3. Agregar la query a la lista de READY
    pthread_mutex_lock(&mutex_ready);
    query_completa->id_query = contador_queries++;
    list_add(ready, query_completa);
    pthread_mutex_unlock(&mutex_ready);

    // Liberar la memoria de la query deserializada
    free(query->archivo_query);
    free(query);

    // 4. Loguear la nueva conexión y "despertar" al planificador
    log_info(logger_master, "## Se conecta un Query Control para ejecutar la Query %s con prioridad %d - Id asignado: %d",
             query_completa->archivo_query, query_completa->prioridad, query_completa->id_query);
    sem_post(&sem_queries_en_ready);
    
    // El hilo termina aquí. La conexión del socket queda abierta para uso futuro.
}

void atender_worker(int socket_cliente, t_paquete* paquete) {
    uint32_t id_worker = deserializar_worker(paquete->buffer);
    liberar_paquete(paquete);

    t_worker_completo* worker = malloc(sizeof(t_worker_completo));
    worker->socket_cliente = socket_cliente;
    worker->id_worker = id_worker;
    worker->libre = true;
    worker->query_asignada = NULL; // Incializa en NULL

    pthread_mutex_lock(&mutex_workers);
    list_add(workers, worker);
    int total_workers = list_size(workers);
    pthread_mutex_unlock(&mutex_workers);
    
    log_info(logger_master, "## Se conecta el Worker %d - Cantidad total de Workers: %d", worker->id_worker, total_workers);
    
   
}