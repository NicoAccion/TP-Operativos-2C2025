#include "atender-hilos.h"

// --- Variables Globales ---
uint32_t contador_queries = 0;
uint32_t id_query_a_eliminar;
uint32_t id_worker_a_eliminar;

t_list* ready;
t_list* exec;
t_list* workers;

// --- Sincronización ---
pthread_mutex_t mutex_ready;
pthread_mutex_t mutex_exec;
pthread_mutex_t mutex_workers;
sem_t sem_queries_en_ready;
sem_t sem_workers_libres;

void inicializar_estructuras_globales() {
    ready = list_create();
    exec = list_create();
    workers = list_create();

    pthread_mutex_init(&mutex_ready, NULL);
    pthread_mutex_init(&mutex_exec, NULL);
    pthread_mutex_init(&mutex_workers, NULL);
    sem_init(&sem_queries_en_ready, 0, 0); // Inicia en 0
    sem_init(&sem_workers_libres, 0, 0); // Inicia en 0
}

void destruir_estructuras_globales() {
    
    list_destroy_and_destroy_elements(ready, free);
    list_destroy_and_destroy_elements(exec, free);
    list_destroy_and_destroy_elements(workers, free);

    pthread_mutex_destroy(&mutex_ready);
    pthread_mutex_destroy(&mutex_exec);
    pthread_mutex_destroy(&mutex_workers);
    sem_destroy(&sem_queries_en_ready);
    sem_destroy(&sem_workers_libres);
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

    // 4. Loguear la nueva conexión y habilita al planificador
    pthread_mutex_lock(&mutex_workers);
    int cantidad_workers = list_size(workers);
    pthread_mutex_unlock(&mutex_workers);
    log_info(logger_master, "## Se conecta un Query Control para ejecutar la Query %s con prioridad %d - Id asignado: %d. Nivel multiprocesamiento %d",
             query_completa->archivo_query, query_completa->prioridad, query_completa->id_query, cantidad_workers);
    sem_post(&sem_queries_en_ready);

    // 5. Verifica si se desconecta el Query Control
    while (1) {
        t_paquete* paquete = recibir_paquete(query_completa->socket_cliente);

        if (paquete == NULL) {
            pthread_mutex_lock(&mutex_workers);
            cantidad_workers = list_size(workers);
            pthread_mutex_unlock(&mutex_workers);
            log_info(logger_master, "## Se desconecta un Query Control. Se finaliza la Query %d con prioridad %d. Nivel multiprocesamiento %d",
                    query_completa->id_query, query_completa->prioridad, cantidad_workers);

            //Cierro la conexión
            close(query_completa->socket_cliente);

            id_query_a_eliminar = query_completa->id_query;

            //Elimino la query de la cola de ready
            if(query_completa->estado == READY){
                list_remove_by_condition (ready, buscar_id_query);
            }

            break;
        }
    }
    
}

bool buscar_id_query(void* arg){
    t_query_completa* query = (t_query_completa*) arg;
    return query->id_query == id_query_a_eliminar;
}

void atender_worker(int socket_cliente, t_paquete* paquete) {
    // 1. Deserializar el worker usando el paquete recibido
    uint32_t id_worker = deserializar_worker(paquete->buffer);
    liberar_paquete(paquete);

    // 2. Crear la estructura interna del worker
    t_worker_completo* worker = malloc(sizeof(t_worker_completo));
    worker->socket_cliente = socket_cliente;
    worker->id_worker = id_worker;
    worker->libre = true;
    worker->query_asignada = NULL; // Incializa en NULL

    // 3. Agregar el worker a la lista de workers
    pthread_mutex_lock(&mutex_workers);
    list_add(workers, worker);
    int cantidad_workers = list_size(workers);
    pthread_mutex_unlock(&mutex_workers);
    
    // 4. Loguear la nueva conexión y habilita al planificador
    log_info(logger_master, "## Se conecta el Worker %d - Cantidad total de Workers: %d", 
             worker->id_worker, cantidad_workers);
    sem_post(&sem_workers_libres);

    // 5. Queda escuchando los mensajes del worker
    while(1){
        t_paquete* paquete = recibir_paquete(worker->socket_cliente);

        //Revisa si se desconectó el worker
        if (paquete == NULL) {
            pthread_mutex_lock(&mutex_workers);
            cantidad_workers = list_size(workers);
            pthread_mutex_unlock(&mutex_workers);
            log_info(logger_master, "## Se desconecta el Worker %d - Se finaliza la Query %d - Cantidad total de Workers: %d ",
                    worker->id_worker, worker->query_asignada->id_query, cantidad_workers);

            //Cierro la conexión
            close(worker->socket_cliente);

            id_worker_a_eliminar = worker->id_worker;

            //Elimino el worker de la lista de workers
            list_remove_by_condition (workers, buscar_id_worker);

            break;
        }
        else{
            switch (paquete->codigo_operacion) {
                case END:
                    enviar_paquete(worker->query_asignada->socket_cliente, paquete);
                    worker->libre = true;
                    sem_post(&sem_workers_libres);
                    break;

                case READ:
                    enviar_paquete(worker->query_asignada->socket_cliente, paquete);
                    break;
            }
        }
    }
}

bool buscar_id_worker(void* arg){
    t_worker_completo* worker = (t_worker_completo*) arg;
    return worker->id_worker == id_worker_a_eliminar;
}
