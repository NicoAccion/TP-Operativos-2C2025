#include "storage.h"
#include "fresh_start.h"

void* atender_worker(void* arg);

int main(int argc, char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Uso: %s [archivo_config]\n", argv[0]);
        return EXIT_FAILURE;
    }

    // 1. Cargar configuración y logger
    inicializar_configs(argv[1]);
    inicializar_logger_storage(storage_configs.loglevel);
    inicializar_superblock_configs(); 
    log_info(logger_storage, "## Storage inicializado.");

    // 2. Inicializar el File System si es FRESH_START
    inicializar_fs(); 

    // 3. Iniciar el servidor
    char* puerto_str = string_itoa(storage_configs.puertoescucha);
    int socket_servidor = iniciar_servidor(puerto_str);
    log_info(logger_storage, "## Storage escuchando en puerto %s", puerto_str);
    free(puerto_str);

    // 4. Aceptar Workers indefinidamente
    while(1) {
        int socket_cliente = esperar_cliente(socket_servidor);
        if (socket_cliente > 0) {
            pthread_t hilo_worker;
            int* cliente_socket_ptr = malloc(sizeof(int));
            *cliente_socket_ptr = socket_cliente;
            pthread_create(&hilo_worker, NULL, (void*) atender_worker, (void*) cliente_socket_ptr);
            pthread_detach(hilo_worker); 
        }
    }

    destruir_logger();
    destruir_configs();
    return EXIT_SUCCESS;
}

void* atender_worker(void* arg) {
    int socket_worker = *((int*) arg);
    free(arg);

    log_info(logger_storage, "## Se conecta un Worker.");

    while(1) {
        t_paquete* paquete = recibir_paquete(socket_worker);

        if (paquete == NULL) {
            log_info(logger_storage, "## Se desconecta un Worker.");
            break; 
        }

        log_info(logger_storage, "Petición recibida del Worker (op_code: %d). Respondiendo OK.", paquete->codigo_operacion);
        
        t_buffer* buffer_respuesta = buffer_create(0);
        t_paquete* paquete_respuesta = empaquetar_buffer(100, buffer_respuesta); // Usamos un op_code genérico para "OK" por ahora
        enviar_paquete(socket_worker, paquete_respuesta);
        
        liberar_paquete(paquete);
    }

    close(socket_worker);
    return NULL;
}