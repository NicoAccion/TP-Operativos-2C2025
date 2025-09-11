#include "query.h"

int main(int argc, char* argv[]) {

    //Valido que se haya ejecutado correctamente
    if (argc != 4) {
        fprintf(stderr, "Uso: %s [archivo_config] [archivo_query] [prioridad]\n", argv[0]);
        return EXIT_FAILURE;
    }

    //Cargo los argumentos en variables
    char* path_config = argv[1];
    char* path_query = argv[2];
    char* prioridad = argv[3];

    saludar("query_control");

    //Inicializo las configs de Query Control
    inicializar_configs(path_config);

    //Inicializo el logger
    inicializar_logger_query(query_configs.loglevel);

    //Creo la conexión con Master
    int socket_master = crear_conexion(query_configs.ipmaster, string_itoa(query_configs.puertomaster));
    printf("Me conecté con Master\n");

    //Envío a Master el path de la query
    printf("Envío a Master el Path de la query: %s\n", path_query);
    enviar_mensaje(path_query, socket_master);

    close(socket_master);
    return 0;
}
