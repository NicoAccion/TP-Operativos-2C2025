#include "master.h"

int main(int argc, char* argv[]) {

    //Valido que se haya ejecutado correctamente
    if (argc != 2) {
        fprintf(stderr, "Uso: %s [archivo_config] \n", argv[0]);
        return EXIT_FAILURE;
    }

    //Cargo el archivo config en una variable
    char* path_config = argv[1];

    saludar("master");

    //Inicializo las configs de Master
    inicializar_configs(path_config);

    //Inicializo el logger
    inicializar_logger_master(master_configs.loglevel);

    //Creo la cola de ready
    ready = list_create();

    //Creo una lista para guardar los workers
    workers = list_create();

    //Inicio el hilo del servidor
    pthread_t hilo_servidor;
    pthread_create(&hilo_servidor, NULL, (void*) servidor_general,  NULL);
    pthread_detach(hilo_servidor);

    //Inicio el hilo del planificador
    pthread_t hilo_planificador;
    pthread_create(&hilo_planificador, NULL, (void*) planificar,  NULL);
    pthread_detach(hilo_planificador);

    while(true){
        sleep(10);
    }

    return 0;
}

void* servidor_general(){

    //Inicio el servidor
    int servidor = iniciar_servidor(string_itoa(master_configs.puertoescucha));

    while(1) {

        //Espero una conexión
        int cliente = esperar_cliente(servidor);

        //Recibo un paquete del cliente
        t_paquete* paquete = recibir_paquete(cliente);

        //Creo una estructura para pasarle a los hilos
        t_conexion* conexion = malloc(sizeof(t_conexion));
        conexion->paquete = paquete;
        conexion->cliente = cliente;

        switch(paquete->codigo_operacion){
            
            //Si se conecta un Query Control
            case PAQUETE_QUERY:

                //Creo un hilo para manejarla
                pthread_t hilo_querycontrol;
                pthread_create(&hilo_querycontrol, NULL, (void*) atender_query_control, (void*) conexion);
                pthread_detach(hilo_querycontrol);
                break;
            
            //Si se conecta un Worker
            case PAQUETE_WORKER:

                //Creo un hilo para manejarlo
                pthread_t hilo_worker;
                pthread_create(&hilo_worker, NULL, (void*) atender_worker, (void*) conexion);
                pthread_detach(hilo_worker);
                break;
        }  
    }
 
    return NULL;
}

