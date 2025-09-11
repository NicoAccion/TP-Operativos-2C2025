#include "storage-configs.h"

//Inicializo los config y el struct global
t_config* storage_tconfig;
storageconfigs storage_configs;

//Inicializo los config del archivo superblock.config y el struct global
t_config* superblock_tconfig;
superblockconfigs superblock_configs;

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                Funciones de las configs de storage

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

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
    free(storage_configs.puntomontaje);
    free(storage_configs.loglevel);

    //Destruyo el config
    config_destroy(storage_tconfig);
    fprintf(stderr, "Archivo de configuración de storage destruido.\n");
}

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                        Funciones de las configs del archivo superblock.configs

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

int inicializar_superblock_configs(){

    //Dirección del archivo superblock.config
    char *direccion = string_from_format("%s/superblock.config", storage_configs.puntomontaje);

    //Creo un config para el archivo superblock.config
    superblock_tconfig = iniciar_config(direccion);
    if (superblock_tconfig == NULL) {
        fprintf(stderr, "Error al cargar el archivo de configuración de superblock.config.\n");
        return EXIT_FAILURE;
    }

    //Libero direccion
    free(direccion);
    
    //Creo el struct donde voy a cargar los datos
    superblockconfigs superblockcargado;

    //Cargo los datos usando las funciones del util de configs.
    superblockcargado.fssize = cargar_variable_int(superblock_tconfig, "FS_SIZE");
    superblockcargado.blocksize = cargar_variable_int(superblock_tconfig, "BLOCK_SIZE");

    //Igualo el struct global a este, de esta forma puedo usar los datos en cualquier archivo del modulo
    superblock_configs = superblockcargado;

    return EXIT_SUCCESS;
}

void destruir_superblock_configs() {
    //Destruyo el config
    config_destroy(superblock_tconfig);
    fprintf(stderr, "Archivo de configuración de superblock.config destruido.\n");
}