#include "storage.h"
#include "fresh_start.h"
#include "storage_conexiones.h"

int main(int argc, char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Uso: %s [archivo_config]\n", argv[0]);
        return EXIT_FAILURE;
    }

    // Cargar configuración y logger
    inicializar_configs(argv[1]);
    inicializar_logger_storage(storage_configs.loglevel);

    log_info(logger_storage, "## Storage inicializado.");

    inicializar_superblock_configs(); 
    
    // Inicializar el File System si es FRESH_START
    inicializar_fs(); 

    // Iniciar el servidor
    char* puerto_str = string_itoa(storage_configs.puertoescucha);
    int socket_servidor = iniciar_servidor(puerto_str);
    log_info(logger_storage, "## Storage escuchando en puerto %s", puerto_str);
    free(puerto_str);

    // Aceptar Workers indefinidamente
    while(1) {
        int socket_cliente = esperar_cliente(socket_servidor);
        if (socket_cliente > 0) {
            pthread_t hilo_worker;
            int* cliente_socket_ptr = malloc(sizeof(int));
            *cliente_socket_ptr = socket_cliente;
            pthread_create(&hilo_worker, NULL, (void*) gestionar_conexion_worker, (void*) cliente_socket_ptr);
            pthread_detach(hilo_worker); 
        }
    }

    destruir_bitmap();
    destruir_logger();
    destruir_configs();
    return EXIT_SUCCESS;
}
