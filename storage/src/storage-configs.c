#include "storage-configs.h"

//Inicializo los config y el struct global
t_config* storage_tconfig;
storageconfigs storage_configs;

int inicializar_configs(){

    //Creo un config para storage
    storage_tconfig = iniciar_config("storage.config");
    if (storage_tconfig == NULL) {
        fprintf(stderr, "Error al cargar el archivo de configuración.\n");
        return EXIT_FAILURE;
    }
    
    //Creo el struct donde voy a cargar los datos
    storageconfigs configcargado;

    //Cargo los datos usando las funciones del util de configs.
    configcargado.puertoescucha = cargar_variable_int(storage_tconfig, "PUERTO_ESCUCHA");
    configcargado.freshstart = cargar_variable_int(storage_tconfig, "FRESH_START");  //Esto tendría que ser bool pero en las commons no hay get_bool_value
    configcargado.puntomontaje = cargar_variable_string(storage_tconfig, "PUNTO_MONTAJE");
    configcargado.retardooperacion = cargar_variable_int(storage_tconfig, "RETARDO_OPERACION");
    configcargado.retardoaccesobloque = cargar_variable_int(storage_tconfig, "RETARDO_ACCESO_BLOQUE");
    configcargado.loglevel = cargar_variable_string(storage_tconfig, "LOG_LEVEL");

    //Igualo el struct global a este, de esta forma puedo usar los datos en cualquier archivo del modulo
    storage_configs = configcargado;

    return EXIT_SUCCESS;
}

void destruir_configs() {
    //Libero memoria

    //Destruyo el config
    config_destroy(storage_tconfig);
    fprintf(stderr, "Archivo de configuración de storage destruido.\n");
}