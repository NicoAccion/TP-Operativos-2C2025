#include "planificacion.h"

void* planificar(){

    //Manejar algoritmo FIFO
    if(strcmp(master_configs.algoritmoplanificacion, "FIFO") == 0){
        fifo();
    }

    //Manejar algoritmo PRIORIDADES
    else if(strcmp(master_configs.algoritmoplanificacion, "PRIORIDADES") == 0){
        prioridades();
    }

    return NULL;
}

void fifo(){
    while(1){
        if(!list_is_empty(ready) && list_any_satisfy(workers, esta_libre)){
            printf("ENTRA AL FIFO\n");

            //Quito la primer query de la cola de ready
            t_query_completa* query = list_remove(ready, 0);

            //La serializo
            t_buffer* buffer = serializar_query_completa(query);
            t_paquete* paquete = empaquetar_buffer(PAQUETE_QUERY_COMPLETA, buffer);

            //Se la envío al primer worker libre
            t_worker_completo* worker_libre = list_find(workers, esta_libre);
            enviar_paquete(worker_libre->socket_cliente, paquete);

            worker_libre->libre = 0;

            //Pongo la query en la lista de exec
            query->estado = EXEC;
            list_add(exec, query);

            log_info(logger_master, "## Se envía la Query %d (%d) al Worker %d",
                    query->id_query, query->prioridad,worker_libre->id_worker);

            
        }
    }
}

bool esta_libre(void* arg){
    t_worker_completo* worker = (t_worker_completo*) arg;
    return worker->libre;
}

void prioridades(){

}