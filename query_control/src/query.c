#include "query.h"

int main(int argc, char* argv[]) {

    //Validación de parámetros - Valido que se haya ejecutado correctamente
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
    if (socket_master == -1) {
        log_error(logger_query, "## Error al conectar con Master en IP: %s, Puerto: %d", query_configs.ipmaster, query_configs.puertomaster);
        return EXIT_FAILURE;
    }
    
    // --- LOG: Conexión con Master ---
    log_info(logger_query, "## Conexión al Master exitosa. IP: %s, Puerto: %d", query_configs.ipmaster, query_configs.puertomaster);


    //Envío a Master el path de la query
    //printf("Envío a Master el Path de la query: %s\n", path_query);
    
    // Concatenar path_query y prioridad en un solo string
    char mensaje_a_enviar[256];
    snprintf(mensaje_a_enviar, sizeof(mensaje_a_enviar), "%s|%s", path_query, prioridad);

    log_info(logger_query, "## Solicitud de ejecución de Query: %s, prioridad: %s", path_query, prioridad);
    enviar_mensaje(mensaje_a_enviar, socket_master);
    
    // --- Escuchar mensajes del Master ---
    escuchar_master(socket_master); 
    

     // --- LOG: Finalización ---
    log_info(logger_query, "## Query Finalizada - Ejecución completada exitosamente");

    close(socket_master);
    return 0;
}
