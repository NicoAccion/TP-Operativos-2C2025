#include "worker-configs.h"

//Inicializo los config y el struct global
t_config* worker_tconfig;
workerconfigs worker_configs;

int inicializar_configs(char* path_config){
    
    //Creo un config para worker
    worker_tconfig = iniciar_config(path_config);
    if (worker_tconfig == NULL) {
        fprintf(stderr, "Error al cargar el archivo de configuración.\n");
        return EXIT_FAILURE;
    }

    //Creo el struct donde voy a cargar los datos
    workerconfigs configcargado;

    //Cargo los datos usando las funciones del util de configs.
    configcargado.ipmaster = cargar_variable_string(worker_tconfig, "IP_MASTER");
    configcargado.puertomaster = cargar_variable_int(worker_tconfig, "PUERTO_MASTER");
    configcargado.ipstorage = cargar_variable_string(worker_tconfig, "IP_STORAGE");
    configcargado.puertostorage = cargar_variable_int(worker_tconfig, "PUERTO_STORAGE");
    configcargado.tammemoria = cargar_variable_int(worker_tconfig, "TAM_MEMORIA");
    configcargado.retardomemoria = cargar_variable_int(worker_tconfig, "RETARDO_MEMORIA");
    configcargado.algoritmoreemplazo = cargar_variable_string(worker_tconfig, "ALGORITMO_REEMPLAZO");
    configcargado.pathqueries = cargar_variable_string(worker_tconfig, "PATH_QUERIES");
    configcargado.loglevel = cargar_variable_string(worker_tconfig, "LOG_LEVEL");


    //Igualo el struct global a este, de esta forma puedo usar los datos en cualquier archivo del modulo
    worker_configs = configcargado;

    return EXIT_SUCCESS;
}

void destruir_configs() {
    //Libero memoria
    free(worker_configs.ipmaster);
    free(worker_configs.ipstorage);
    free(worker_configs.algoritmoreemplazo);
    free(worker_configs.pathqueries);
    free(worker_configs.loglevel);

    //Destruyo el config
    config_destroy(worker_tconfig);
    fprintf(stderr, "Archivo de configuración de worker destruido.\n");
}