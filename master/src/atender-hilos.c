#include "atender-hilos.h"

// --- Variables Globales ---
uint32_t contador_queries;

t_list* ready;
t_list* exec;
t_list* workers;

t_temporal* cronometro;

// --- Sincronización ---
pthread_mutex_t mutex_ready;
pthread_mutex_t mutex_exec;
pthread_mutex_t mutex_workers;
sem_t sem_queries_en_ready;
sem_t sem_workers_libres;
sem_t sem_planificar_prioridad;

void inicializar_estructuras_globales() {
    contador_queries = 0;

    ready = list_create();
    exec = list_create();
    workers = list_create();

    cronometro = temporal_create();

    pthread_mutex_init(&mutex_ready, NULL);
    pthread_mutex_init(&mutex_exec, NULL);
    pthread_mutex_init(&mutex_workers, NULL);
    sem_init(&sem_queries_en_ready, 0, 0); // Inicia en 0
    sem_init(&sem_workers_libres, 0, 0); // Inicia en 0
    sem_init(&sem_planificar_prioridad, 0, 0); //Inicia en 0
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
    sem_destroy(&sem_planificar_prioridad);
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
    query_completa->program_counter = 0;

    // 3. Agregar la query a la lista de READY
    pthread_mutex_lock(&mutex_ready);
    query_completa->id_query = contador_queries++;
    list_add(ready, query_completa);
    pthread_mutex_unlock(&mutex_ready);

    // Liberar la memoria de la query deserializada
    free(query->archivo_query);
    free(query);

    // 4. Loggear la nueva conexión
    pthread_mutex_lock(&mutex_workers);
    int cantidad_workers = list_size(workers);
    pthread_mutex_unlock(&mutex_workers);
    log_info(logger_master, "## Se conecta un Query Control para ejecutar la Query %s con prioridad %d - Id asignado: %d. Nivel multiprocesamiento %d",
             query_completa->archivo_query, query_completa->prioridad, query_completa->id_query, cantidad_workers);

    // 5. Habilita al planificador
    if(strcmp(master_configs.algoritmoplanificacion, "FIFO") == 0){
        sem_post(&sem_queries_en_ready);
    }
    if(strcmp(master_configs.algoritmoplanificacion, "PRIORIDADES") == 0){
        sem_post(&sem_planificar_prioridad);
        query_completa->entrada_a_ready = temporal_gettime(cronometro);
    }

    // 6. Verifica si se desconecta el Query Control
    while (true) {
        t_paquete* paquete_query = recibir_paquete(query_completa->socket_cliente);

        if (paquete_query == NULL) {

            //Si estaba en EXEC le aviso al worker
            if(query_completa->estado == EXEC){
                t_worker_completo* worker = buscar_worker_asignado(query_completa);
                t_paquete* paquete_worker = empaquetar_buffer(DESCONEXION_QUERY, NULL);
                log_info(logger_master, "## Se desaloja la Query %d (%d) del Worker %d - Motivo: DESCONEXION",
                         query_completa->id_query, query_completa->prioridad, worker->id_worker);

                //Verifico si el worker se desconectó
                if(enviar_paquete(worker->socket_cliente, paquete_worker) == -1){

                    //Elimino el worker de la lista de workers
                    pthread_mutex_lock(&mutex_workers);
                    list_remove_element(workers, worker);
                    cantidad_workers --;
                    pthread_mutex_unlock(&mutex_workers);

                    //Loggeo la desconexión
                    pthread_mutex_lock(&mutex_workers);
                    cantidad_workers = list_size(workers);
                    pthread_mutex_unlock(&mutex_workers);

                    log_info(logger_master, "## Se desconecta el Worker %d - Se finaliza la Query %d - Cantidad total de Workers: %d ",
                         worker->id_worker, worker->query_asignada->id_query, cantidad_workers);

                    //Cierro la conexión del worker
                    close(worker->socket_cliente);

                    //Libero el worker
                    free(worker);

                    //Finalizo la query
                    finalizar_query(query_completa);
                };
            }

            //Si estaba en READY la finalizo directamente
            if(query_completa->estado == READY){
                pthread_mutex_lock(&mutex_workers);
                cantidad_workers = list_size(workers);
                pthread_mutex_unlock(&mutex_workers);

                log_info(logger_master, "## Se desconecta un Query Control. Se finaliza la Query %d con prioridad %d. Nivel multiprocesamiento %d",
                         query_completa->id_query, query_completa->prioridad, cantidad_workers);

                finalizar_query(query_completa);
            }

            break;
        }
    }
}

t_worker_completo* buscar_worker_asignado(t_query_completa* query){
    pthread_mutex_lock(&mutex_workers);
    for (int i = 0; i < list_size(workers); i++){
        t_worker_completo* worker = list_get(workers, i);
        if (worker->query_asignada == query){
            pthread_mutex_unlock(&mutex_workers);
            return worker;
        }
    }
    pthread_mutex_unlock(&mutex_workers);
    return NULL;
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

    // 4. Loggear la nueva conexión
    log_info(logger_master, "## Se conecta el Worker %d - Cantidad total de Workers: %d", 
             worker->id_worker, cantidad_workers);

    // 5. Habilita al planificador
    if(strcmp(master_configs.algoritmoplanificacion, "FIFO") == 0){
        sem_post(&sem_workers_libres);
    }
    if(strcmp(master_configs.algoritmoplanificacion, "PRIORIDADES") == 0){
        sem_post(&sem_planificar_prioridad);
    }

    // 6. Queda escuchando los mensajes del worker
    while(true){
        t_paquete* paquete = recibir_paquete(worker->socket_cliente);

        //Revisa si se desconectó el worker
        if (paquete == NULL) {

            //Cierro la conexión
            close(worker->socket_cliente);

            //Si no tiene query asignada finalizo el worker
            if (worker->libre == true){

                //Elimino el worker de la lista
                pthread_mutex_lock(&mutex_workers);
                list_remove_element (workers, worker);
                pthread_mutex_unlock(&mutex_workers);

                //Loggeo la desconexión
                pthread_mutex_lock(&mutex_workers);
                cantidad_workers = list_size(workers);
                pthread_mutex_unlock(&mutex_workers);

                log_info(logger_master, "## Se desconecta el Worker %d - Sin Query asignada - Cantidad total de Workers: %d ",
                         worker->id_worker, cantidad_workers);

                // Libero la memoria
                free(worker);

            //Sino le aviso a la query
            } else {
                t_buffer* buffer = serializar_operacion_end("DESCONEXION WORKER");
                t_paquete* paquete = empaquetar_buffer(END, buffer);
                enviar_paquete(worker->query_asignada->socket_cliente, paquete);
            }

            break;
        }
        else{
            switch (paquete->codigo_operacion) {

                //Recibe fin de query
                case END:
                    char* motivo = deserializar_operacion_end(paquete->buffer);

                    //Si se ejecuta correctamente
                    if(strcmp(motivo, "OK") == 0){
                        log_info(logger_master, "## Se terminó la Query %d en el Worker %d",
                                 worker->query_asignada->id_query, worker->id_worker);

                        worker->query_asignada->estado = EXIT;

                        enviar_paquete(worker->query_asignada->socket_cliente, paquete);
                    }

                    //Si se desconecta el Query Control
                    else if(strcmp(motivo, "DESCONEXION QUERY") == 0){
                        pthread_mutex_lock(&mutex_workers);
                        cantidad_workers = list_size(workers);
                        pthread_mutex_unlock(&mutex_workers);

                        log_info(logger_master, "## Se desconecta un Query Control. Se finaliza la Query %d con prioridad %d. Nivel multiprocesamiento %d",
                                 worker->query_asignada->id_query, worker->query_asignada->prioridad, cantidad_workers);

                        liberar_paquete(paquete);

                        finalizar_query(worker->query_asignada);
                    }

                    //En caso de algún error
                    else {
                        pthread_mutex_lock(&mutex_workers);
                        cantidad_workers = list_size(workers);
                        pthread_mutex_unlock(&mutex_workers);

                        log_info(logger_master, "## Se desconecta un Query Control. Se finaliza la Query %d con prioridad %d. Nivel multiprocesamiento %d",
                                 worker->query_asignada->id_query, worker->query_asignada->prioridad, cantidad_workers);

                        enviar_paquete(worker->query_asignada->socket_cliente, paquete);
                    }

                    free(motivo);

                    worker->libre = true;
                   //worker->query_asignada = NULL;

                    //Habilita al planificador
                    if(strcmp(master_configs.algoritmoplanificacion, "FIFO") == 0){
                        sem_post(&sem_workers_libres);
                    }
                    if(strcmp(master_configs.algoritmoplanificacion, "PRIORIDADES") == 0){
                        sem_post(&sem_planificar_prioridad);
                    }

                    break;

                //Recibe información leída por la Query
                case READ:
                    enviar_paquete(worker->query_asignada->socket_cliente, paquete);

                    log_info(logger_master, "## Se envía un mensaje de lectura de la Query %d en el Worker %d al Query Control",
                             worker->query_asignada->id_query, worker->id_worker);

                    break;

                //Recibe un desalojo por prioridades
                case DESALOJO_PRIORIDADES:
                    t_query_ejecucion* query = deserializar_query_ejecucion(paquete->buffer);

                    //Actualizo el program counter de la query
                    worker->query_asignada->program_counter = query->program_counter;

                    //La quito de la lista de exec
                    pthread_mutex_lock(&mutex_exec);
                    list_remove_element(exec, worker->query_asignada);
                    pthread_mutex_unlock(&mutex_exec);

                    //La agrego a la cola de ready
                    pthread_mutex_lock(&mutex_ready);
                    list_add(ready, worker->query_asignada);
                    pthread_mutex_unlock(&mutex_ready);

                    worker->query_asignada->estado = READY;
                    worker->query_asignada->entrada_a_ready = temporal_gettime(cronometro);

                    //Loggeo el desalojo
                    log_info(logger_master, "## Se desaloja la Query %d (%d) del Worker %d - Motivo: PRIORIDAD",
                             worker->query_asignada->id_query, worker->query_asignada->prioridad, worker->id_worker);

                    worker->libre = true;

                    //Habilita al planificador                 
                    sem_post(&sem_planificar_prioridad);
                    
                    
                    //Libero memoria
                    free(query);
                    liberar_paquete(paquete);

                    break;

                default:
                    log_warning(logger_master, "## Código de operación inesperado (%d) recibido del Worker %d",
                    paquete->codigo_operacion, worker->id_worker);
                    liberar_paquete(paquete);
                    break;              
            }
        }
    }
}

void finalizar_query(t_query_completa* query){
    //Cierro la conexión
    close(query->socket_cliente);

    //Elimino la query de la lista en la que esté, si no está en ninguna lista no hace nada
    if(query->estado == READY){
        pthread_mutex_lock(&mutex_ready);
        list_remove_element (ready, query);
        pthread_mutex_unlock(&mutex_ready);
    } else {
        if(query->estado == EXEC){
            pthread_mutex_lock(&mutex_exec);
            list_remove_element (exec, query);
            pthread_mutex_unlock(&mutex_exec);
        }
    }

    // Libero la memoria
    free(query->archivo_query);
    free(query);
}
