#include "worker.h"

int main(int argc, char* argv[]) {

    //Valido que se haya ejecutado correctamente
    if (argc != 3) {
        fprintf(stderr, "Uso: %s [archivo_config] [ID worker]\n", argv[0]);
        return EXIT_FAILURE;
    }

    //Cargo los argumentos en variables
    char* path_config = argv[1];
    char* id_worker = argv[2];

    saludar("worker");

    //Inicializo las configs de Worker
    inicializar_configs(path_config);

    //Creo la conexión con Master
    int socket_master = crear_conexion(worker_configs.ipmaster, string_itoa(worker_configs.puertomaster));
    printf("Me conecté con Master\n");

    //Recibo el Path del query que me envía el Master
    char* path_query = recibir_mensaje(socket_master);
    if (path_query != NULL) {
        printf("Path del Query que recibí de Master: %s\n", path_query);
    }

    close(socket_master);

    //Creo la conexión con Storage
    int socket_storage = crear_conexion(worker_configs.ipstorage, string_itoa(worker_configs.puertostorage));
    printf("Me conecté con Storage\n");

    //Solicito el tamaño del bloque a Storage
    printf("Solicito a Storage el tamaño del bloque\n");
    enviar_mensaje("Pasame el tamaño del bloque", socket_storage);

    //Recibo el tamaño solicitado
    char* tam_bloque = recibir_mensaje(socket_master);
    if (tam_bloque != NULL) {
        printf("Tamaño del bloque que recibí de Storage: %s\n", tam_bloque);
    }

    close(socket_storage);

    return 0;
}