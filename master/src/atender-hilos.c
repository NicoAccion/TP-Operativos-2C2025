#include "atender-hilos.h"

uint32_t contador_queries = 0;

uint32_t contador_workers = 0;

uint32_t id_query_a_eliminar;

uint32_t id_worker_a_eliminar;

t_list* ready = NULL;

t_list* exec = NULL;

t_list* workers = NULL;

void* atender_query_control(void* arg){
    
    //Deserializo la query
    t_conexion* conexion = (t_conexion*) arg;
    t_query* query = deserializar_query(conexion->paquete->buffer);

    //Le asigno un identificador y el estado
    t_query_completa* query_completa = malloc(sizeof(t_query_completa));
    query_completa->socket_cliente = conexion->cliente;
    query_completa->archivo_query = query->archivo_query; 
    query_completa->prioridad = query->prioridad;
    query_completa->id_query = contador_queries;
    query_completa->estado = READY;

    log_info(logger_master, "## Se conecta un Query Control para ejecutar la Query %s con prioridad %d - Id asignado: %d. Nivel multiprocesamiento %d",
            query_completa->archivo_query, query_completa->prioridad, query_completa->id_query, contador_workers);

    //Incremento el contador de queries
    contador_queries++;
    
    //Pongo la Query en la cola de Ready
    list_add(ready, query_completa);

    /*t_query_completa* prueba = list_remove(ready, 0);
    printf("PRIMER ELEMENTO DE LA COLA. QUERY: %s PRIORIDAD: %d ID: %d ESTADO: %d\n", prueba->archivo_query, prueba->prioridad, prueba->id_query, prueba->estado);
    fflush(stdout);*/

    while (1) {
        t_paquete* paquete = recibir_paquete(conexion->cliente);

        //Revisa si se desconectó el query control
        if (paquete == NULL) {

            log_info(logger_master, "## Se desconecta un Query Control. Se finaliza la Query %d con prioridad %d. Nivel multiprocesamiento %d",
                    query_completa->id_query, query_completa->prioridad, contador_workers);

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

    return NULL;
}

bool buscar_id_query(void* arg){
    t_query_completa* query = (t_query_completa*) arg;
    return query->id_query == id_query_a_eliminar;
}

void* atender_worker(void* arg){

    //Deserializo el worker
    t_conexion* conexion = (t_conexion*) arg;
    uint32_t id_worker = deserializar_worker(conexion->paquete->buffer);

    //Le asigno un bool para saber si esta libre
    t_worker_completo* worker_completo = malloc(sizeof(t_worker_completo));
    worker_completo->socket_cliente = conexion->cliente;
    worker_completo->id_worker = id_worker;
    worker_completo->libre = 1;

    //Pongo el worker en la lista de workers
    list_add(workers, worker_completo);

    //Incremento el contador de workers
    contador_workers++;

    log_info(logger_master, "## Se conecta el Worker %d - Cantidad total de Workers: %d", worker_completo->id_worker, contador_workers);

    /*t_worker_completo* prueba = list_remove(workers, 0);
    printf("PRIMER ELEMENTO DE LA COLA. ID WORKER: %d LIBRE: %d\n", prueba->id_worker, prueba->libre);
    fflush(stdout);*/

    //Creo el hilo que recibe mensajes de las lecturas realizadas por el Worker
    pthread_t hilo_escucha;
    pthread_create(&hilo_escucha, NULL, (void*) escuchar_worker, (void*) worker_completo);
    pthread_detach(hilo_escucha);

    return NULL;
}

void* escuchar_worker(void* arg){
    t_worker_completo* worker = (t_worker_completo*) arg;
    while(1){
        t_paquete* paquete = recibir_paquete(worker->socket_cliente);

        //Revisa si se desconectó el worker
        if (paquete == NULL) {

            //Cierro la conexión
            close(worker->socket_cliente);

            id_worker_a_eliminar = worker->id_worker;

            //Elimino el worker de la lista de workers
            list_remove_by_condition (workers, buscar_id_worker);

            contador_workers--;

            log_info(logger_master, "## Se desconecta el Worker %d - Se finaliza la Query %d - Cantidad total de Workers: %d ",
                    worker->id_worker, worker->query.id_query, contador_workers);

            break;
        }
        else{
            switch (paquete->codigo_operacion) {

                case END:
                    worker->libre = 1;
                    enviar_paquete(worker->query.socket_cliente, paquete);
                    break;

                case READ:
                    enviar_paquete(worker->query.socket_cliente, paquete);
                    break;
            }
        }
    }

    return NULL;
}

bool buscar_id_worker(void* arg){
    t_worker_completo* worker = (t_worker_completo*) arg;
    return worker->id_worker == id_worker_a_eliminar;
}
