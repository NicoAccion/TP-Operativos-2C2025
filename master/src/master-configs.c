#include "master-configs.h"

//Inicializo los config y el struct global
t_config* master_tconfig;
masterconfigs master_configs;

int inicializar_configs(){

    //Creo un config para master
    master_tconfig = iniciar_config("master.config");
    if (master_tconfig == NULL) {
        fprintf(stderr, "Error al cargar el archivo de configuración.\n");
        return EXIT_FAILURE;
    }

    //Creo el struct donde voy a cargar los datos
    masterconfigs configcargado;
    
    //Cargo los datos en el, usando las funciones del util de configs.
    configcargado.puertoescucha = cargar_variable_int(master_tconfig, "PUERTO_ESCUCHA");
    configcargado.algoritmoplanificacion = cargar_variable_string(master_tconfig, "ALGORITMO_PLANIFICACION");
    configcargado.tiempoaging = cargar_variable_int(master_tconfig, "TIEMPO_AGING");
    configcargado.loglevel = cargar_variable_string(master_tconfig, "LOG_LEVEL");


    //Igualo el struct global a este, de esta forma puedo usar los datos en cualquier archivo del modulo
    master_configs = configcargado;

    return EXIT_SUCCESS;
}

void destruir_configs() {
    //Libero memoria
    free(master_configs.algoritmoplanificacion);
    free(master_configs.loglevel);

    //Destruyo el config
    config_destroy(master_tconfig);
    fprintf(stderr, "Archivo de configuración de master destruido.\n");
}