#include "master.h"

int main(int argc, char* argv[]) {
    // 1. Validación de Parámetros
    if (argc != 2) {
        fprintf(stderr, "Uso: %s [archivo_config]\n", argv[0]);
        return EXIT_FAILURE;
    }

    // 2. Inicialización de Configs y Logger
    inicializar_configs(argv[1]);
    inicializar_logger_master(master_configs.loglevel);
    log_info(logger_master, "## Master inicializado.");

    // 3. Inicialización de Estructuras Globales
    inicializar_estructuras_globales();

    // 4. Lanzamiento de Hilos Principales
    pthread_t hilo_servidor;
    pthread_t hilo_planificador;

    pthread_create(&hilo_servidor, NULL, (void*) servidor_general, NULL);

    pthread_create(&hilo_planificador, NULL, (void*) planificar, NULL);

    // 5. Esperar a que los hilos terminen (en este caso, nunca)
    pthread_join(hilo_servidor, NULL);
    pthread_join(hilo_planificador, NULL);

    // 6. Limpieza Final (no se alcanzará en esta implementación)
    destruir_estructuras_globales();
    destruir_logger();
    destruir_configs();

    return EXIT_SUCCESS;
}

void* servidor_general() {
    char* puerto_str = string_itoa(master_configs.puertoescucha);
    int socket_servidor = iniciar_servidor(puerto_str);
    log_info(logger_master, "## Master escuchando en puerto %s", puerto_str);
    free(puerto_str);

    while(1) {
        int socket_cliente = esperar_cliente(socket_servidor);
        
        // Se crea hilo para manejar el handshake y decidir qué tipo de cliente es
        pthread_t hilo_cliente;
        int* cliente_socket_ptr = malloc(sizeof(int));
        *cliente_socket_ptr = socket_cliente;
        pthread_create(&hilo_cliente, NULL, (void*) manejar_nueva_conexion, (void*) cliente_socket_ptr);
        pthread_detach(hilo_cliente);
    }

    return NULL;
}