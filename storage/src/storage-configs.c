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

int inicializar_configs(char* path){

    //Creo un config para storage
    storage_tconfig = iniciar_config(path);
    if (storage_tconfig == NULL) {
        fprintf(stderr, "Error al cargar el archivo de configuración.\n");
        return EXIT_FAILURE;
    }
    
    //Creo el struct donde voy a cargar los datos
    storageconfigs configcargado;

    //Cargo los datos usando las funciones del util de configs.
    configcargado.puertoescucha = cargar_variable_int(storage_tconfig, "PUERTO_ESCUCHA");
    configcargado.freshstart = cargar_variable_bool(storage_tconfig, "FRESH_START");
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

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                        Funciones de las configs del archivo metadata.config

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/


char* convertir_lista_a_string(t_list* lista) {
    char* string_bloques = string_new();
    string_append(&string_bloques, "[");

    for (int i = 0; i < list_size(lista); i++) {
        uint32_t* bloque_ptr = list_get(lista, i);
        char* numero_str = string_itoa((int)*bloque_ptr);

        string_append(&string_bloques, numero_str);
        
        if (i < list_size(lista) - 1) {
            string_append(&string_bloques, ",");
        }
        free(numero_str);
    }
    
    string_append(&string_bloques, "]");
    return string_bloques;
}

void guardar_metadata_en_archivo(metadataconfigs* metadata, char* path) {
     // 1. Armo la ruta absoluta.
    char ruta_absoluta[1024];
    snprintf(ruta_absoluta, 
             sizeof(ruta_absoluta), 
             "%s/files/%s", 
             storage_configs.puntomontaje, 
             path);
    
    t_config* config = malloc(sizeof(t_config));
    config->path = strdup(ruta_absoluta);
    config->properties = dictionary_create();

    // 2. Convertimos cada campo del struct a string y lo agregamos al config
    char* tamanio_str = string_itoa(metadata->tamanio);
    char* bloques_str = convertir_lista_a_string(metadata->blocks);

    config_set_value(config, "TAMANIO", tamanio_str);
    config_set_value(config, "ESTADO", metadata->estado);
    config_set_value(config, "BLOQUES", bloques_str);

    // 3. Guardamos la configuración en el archivo. Esto lo crea si no existe.
    config_save_in_file(config, ruta_absoluta);
    printf("INFO: Metadata guardada correctamente en '%s'\n", ruta_absoluta);

    // 4. Liberamos toda la memoria temporal que usamos
    free(tamanio_str);
    free(bloques_str);
    config_destroy(config);
}
