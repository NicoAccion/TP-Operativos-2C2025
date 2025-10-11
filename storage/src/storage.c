#include "storage.h"

int main(int argc, char* argv[]) {

    //Valido que se haya pasado el path del archivo config
    if (argc != 2) {
        fprintf(stderr, "Uso: %s [archivo_config]\n", argv[0]);
        return EXIT_FAILURE;
    }
    char* path_config = argv[1];

    //Inicializo las configs de Storage
    inicializar_configs(path_config);

    //Inicializo las configs del archivo superblock.config
    inicializar_superblock_configs();

    //Inicializo el logger
    inicializar_logger_storage(storage_configs.loglevel);

    //Configuro el FS segun config
    inicializar_fs();
    
    //Inicio el servidor
    int storage_server = iniciar_servidor(string_itoa(storage_configs.puertoescucha));

    //Espero la conexión de Worker
    int socket_worker = esperar_cliente(storage_server);

    //Recibo la solicitud de Worker por el tamaño de bloque
    char* solicitud = recibir_mensaje(socket_worker);
    if (string_equals_ignore_case(solicitud, "Pasame el tamaño del bloque") == 1){
        printf("Recibí la solicitud de parte de Worker\n");
    }

    //Envío el tamaño solicitado a Worker
    printf("Envío a Worker el tamaño del bloque: %d\n", superblock_configs.blocksize);
    enviar_mensaje(string_itoa(superblock_configs.blocksize), socket_worker);

    return 0;
}
