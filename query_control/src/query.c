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
    uint32_t prioridad = atoi(argv[3]);
    
    //Inicializo las configs de Query Control
    inicializar_configs(path_config);

    //Inicializo el logger
    inicializar_logger_query(query_configs.loglevel);

    //Creo la conexión con Master
    char* puerto = string_itoa(query_configs.puertomaster);
    int socket_master = crear_conexion(query_configs.ipmaster, puerto);
    free(puerto);

    if (socket_master == -1) {
        log_error(logger_query, "## Error al conectar con Master en IP: %s, Puerto: %d",
                  query_configs.ipmaster, query_configs.puertomaster);
        log_destroy(logger_query);
        return EXIT_FAILURE;
    }

    log_info(logger_query, "## Conexión al Master exitosa. IP: %s, Puerto: %d",
             query_configs.ipmaster, query_configs.puertomaster);
    
    //Serializo el path de la query y la prioridad
    t_query query = {path_query, prioridad};
    t_buffer* buffer = serializar_query(&query);
    t_paquete* paquete = empaquetar_buffer(HANDSHAKE_QUERYCONTROL, buffer);

    //Se lo envío a Master
    enviar_paquete(socket_master, paquete);
    log_info(logger_query, "## Solicitud de ejecución de Query: %s, prioridad: %d", path_query, prioridad);

    escuchar_master(socket_master);

    close(socket_master);
    log_destroy(logger_query);
    return 0;
}

void escuchar_master(int socket_master) {
    while(true) {

        t_paquete* paquete = recibir_paquete(socket_master);

        if (paquete  == NULL) {
            log_error(logger_query, "## Conexión con Master perdida.");
            break;
        }

        switch(paquete->codigo_operacion){
            case READ:
                t_operacion_query* operacion = deserializar_operacion_query(paquete->buffer);
                log_info(logger_query, "## Lectura realizada: File %s:%s, contenido: %s",
                         operacion->file, operacion->tag, operacion->informacion);
                destruir_operacion_query(operacion);
                liberar_paquete(paquete);
                break;

            case END:
                char* motivo = deserializar_operacion_end(paquete->buffer);
                log_info(logger_query, "## Query finalizada: %s", motivo);
                free(motivo);
                liberar_paquete(paquete);
                return;

            default:
                log_warning(logger_query,"## Código de operación desconocido recibido: %d",paquete->codigo_operacion);
                liberar_paquete(paquete);
                break;
        }
    }
}