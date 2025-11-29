#include "planificacion.h"

void* planificar(){

    //Manejar algoritmo FIFO
    if(strcmp(master_configs.algoritmoplanificacion, "FIFO") == 0){
        fifo();
    }

    //Manejar algoritmo PRIORIDADES
    else if(strcmp(master_configs.algoritmoplanificacion, "PRIORIDADES") == 0){
        
        //Creo el hilo encargado del aging
        pthread_t hilo_aging;
        pthread_create(&hilo_aging, NULL, (void*) aging, NULL);

        prioridades();
    }

    return NULL;
}

void fifo(){
    while(1){
        //Se habilita cuando hay un query en ready y un worker libre
        sem_wait(&sem_queries_en_ready);
        sem_wait(&sem_workers_libres);

        //Busco la primer query de la cola de ready
        pthread_mutex_lock(&mutex_ready);
        t_query_completa* query = list_get(ready, 0);
        pthread_mutex_unlock(&mutex_ready);

        //Se la asigno al primer worker libre
        asignar_query(query);
    }
}

void prioridades(){
    while (1) {
        //Se habilita cuando entra un query a ready, cuando se libera un worker o después de realizar el aging
        sem_wait(&sem_planificar_prioridad);

        //Primero reviso que haya almenos una query en ready y un worker conectado
        if(list_size(ready) > 0 && list_size(workers) > 0){

            //Busco a la query en ready con mayor prioridad
            pthread_mutex_lock(&mutex_ready);
            t_query_completa* query_maxima = list_get_maximum(ready, mayor_prioridad);
            pthread_mutex_unlock(&mutex_ready);

            //Si hay un worker libre lo asigno directamente
            if(list_any_satisfy (workers, esta_libre)){
                asignar_query(query_maxima);
            }

            //Sino reviso si se está ejecutando una query de menor prioridad
            else{

                //Busco a la query en exec con menor prioridad
                pthread_mutex_lock(&mutex_exec);
                t_query_completa* query_minima = list_get_minimum(exec, menor_prioridad);
                pthread_mutex_unlock(&mutex_exec);
                
                if(query_maxima->prioridad < query_minima->prioridad){

                    //Le pido al worker que la desaloje
                    t_worker_completo* worker = buscar_worker_asignado(query_minima);
                    t_paquete* paquete_worker = empaquetar_buffer(DESALOJO_PRIORIDADES, NULL);
                    enviar_paquete(worker->socket_cliente, paquete_worker);
                }
            }
        }
    }
}

void asignar_query(t_query_completa* query){

    //Quito la query de la lista ready
    pthread_mutex_lock(&mutex_ready);
    list_remove_element(ready, query);
    pthread_mutex_unlock(&mutex_ready);

    //Creo la estructura para enviarla al worker
    t_query_ejecucion* query_ejecucion = malloc(sizeof(t_query_ejecucion));
    query_ejecucion->archivo_query = strdup(query->archivo_query);
    query_ejecucion->id_query = query->id_query;
    query_ejecucion->program_counter = query->program_counter;

    t_buffer* buffer = serializar_query_ejecucion(query_ejecucion);
    t_paquete* paquete = empaquetar_buffer(PAQUETE_QUERY_EJECUCION, buffer);

    //Se la envío al primer worker libre
    t_worker_completo* worker_libre = list_find(workers, esta_libre);
    enviar_paquete(worker_libre->socket_cliente, paquete);

    worker_libre->libre = false;
    worker_libre->query_asignada = query;

    //Pongo la query en la lista de exec
    query->estado = EXEC;
    list_add(exec, query);

    //Loggeo
    log_info(logger_master, "## Se envía la Query %d (%d) al Worker %d",
    query->id_query, query->prioridad, worker_libre->id_worker);

    //Libero memoria
    free(query_ejecucion->archivo_query);
    free(query_ejecucion);
}

bool esta_libre(void* arg){
    t_worker_completo* worker = (t_worker_completo*) arg;
    return worker->libre;
}

void* mayor_prioridad(void* arg1, void* arg2){
    t_query_completa* query1 = (t_query_completa*) arg1;
    t_query_completa* query2 = (t_query_completa*) arg2;
    if(query1->prioridad <= query2->prioridad){
        return query1;
    }
    else{
        return query2;
    }
}

void* menor_prioridad(void* arg1, void* arg2){
    t_query_completa* query1 = (t_query_completa*) arg1;
    t_query_completa* query2 = (t_query_completa*) arg2;
    if(query1->prioridad >= query2->prioridad){
        return query1;
    }
    else{
        return query2;
    }
}

void* aging(){

    //Inicio el cronómetro general
    t_temporal* cronometro = temporal_create();

    while (1){

        usleep(50 * 1000);

        int64_t ahora = temporal_gettime(cronometro);

        //Aumenta la prioridad de las queries en ready
        pthread_mutex_lock(&mutex_ready);
        for(int i = 0; i < list_size(ready); i++) {
            t_query_completa* query = list_get(ready, i);

            int64_t espera = ahora - query->entrada_a_ready;
            //log_warning(logger_master, "AHORA: %ld, ESPERA: %ld, AGING EN MILISEGUNDOS: %ld, ENTRADA A READY: %ld",
            //    ahora, espera, aging_en_milisegundos, query->entrada_a_ready);
            if (query->prioridad > 1 && espera >= master_configs.tiempoaging) {
                query->prioridad--;
                log_warning(logger_master, "##%d Cambio de prioridad: %d - %d", 
                         query->id_query, query->prioridad + 1, query->prioridad);
                query->entrada_a_ready = ahora;
            }
        }
        pthread_mutex_unlock(&mutex_ready);
        sem_post(&sem_planificar_prioridad);
    }
}