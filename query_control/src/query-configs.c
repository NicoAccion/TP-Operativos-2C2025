#include "query-configs.h"

//Inicializo los config y el struct global
t_config* query_tconfig;
queryconfigs query_configs;

int inicializar_configs(){
    //Creo un config para query
    query_tconfig = iniciar_config("query.config");
    if (query_tconfig == NULL) {
        fprintf(stderr, "Error al cargar el archivo de configuración.\n");
        return EXIT_FAILURE;
    }
    //Creo el struct donde voy a cargar los datos
    queryconfigs configcargado;
    //Cargo los datos usando las funciones del util de configs.
    configcargado.ipmaster = cargar_variable_string(query_tconfig, "IP_MASTER");
    configcargado.puertomaster = cargar_variable_int(query_tconfig, "PUERTO_MASTER");
    configcargado.loglevel = cargar_variable_string(query_tconfig, "LOG_LEVEL");


    //Igualo el struct global a este, de esta forma puedo usar los datos en cualquier archivo del modulo
    query_configs = configcargado;

    return EXIT_SUCCESS;
}

void destruir_configs() {
    //Libero memoria

    //Destruyo el config
    config_destroy(query_tconfig);
    fprintf(stderr, "Archivo de configuración de query destruido.\n");
}