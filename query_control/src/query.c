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


    saludar("query_control");

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

    // --- LOG: Conexión con Master ---
    log_info(logger_query, "## Conexión al Master exitosa. IP: %s, Puerto: %d",
             query_configs.ipmaster, query_configs.puertomaster);

     // Concatenar path_query y prioridad en un solo string
    // Envío de Query
    char* mensaje_a_enviar = string_from_format("%s|%s", path_query, prioridad);
    log_info(logger_query, "## Solicitud de ejecución de Query: %s, prioridad: %s",
             path_query, prioridad);

    if (enviar_mensaje(mensaje_a_enviar, socket_master) <= 0) {
        log_error(logger_query, "## Error al enviar Query al Master.");
        free(mensaje_a_enviar);
        close(socket_master);
        log_destroy(logger_query);
        return EXIT_FAILURE;
    }
    free(mensaje_a_enviar);

    

    // Escuchar mensajes del Master
    escuchar_master(socket_master);

    close(socket_master);
    log_destroy(logger_query);
    return 0;
}

void escuchar_master(int socket_master) {
    while (1) {
        char* mensaje = recibir_mensaje(socket_master);
        if (mensaje == NULL) {
            log_error(logger_query, "## Conexión con Master perdida.");
            break;
        }

        if (strncmp(mensaje, "READ", 4) == 0) {
            char** partes = string_split(mensaje, "|");
            char* file = NULL;
            char* tag = NULL;
            char* contenido = NULL;

            for (int i = 0; partes[i] != NULL; i++) {
                if (string_starts_with(partes[i], "File:"))
                    file = partes[i] + strlen("File:");
                else if (string_starts_with(partes[i], "Tag:"))
                    tag = partes[i] + strlen("Tag:");
                else if (string_starts_with(partes[i], "Contenido:"))
                    contenido = partes[i] + strlen("Contenido:");
            }

            log_info(logger_query, "## Lectura realizada: File %s:%s, contenido: %s",
                     file ? file : "?", tag ? tag : "?", contenido ? contenido : "?");

            string_array_destroy(partes);
        }

        else if (strncmp(mensaje, "END", 3) == 0) {
            char** partes = string_split(mensaje, "|");
            char* motivo = partes[1] ? partes[1] : "Finalización desconocida";

            log_info(logger_query, "## Query Finalizada - %s", motivo);

            string_array_destroy(partes);
            free(mensaje);
            break;
        }

        else {
            log_warning(logger_query, "## Mensaje desconocido recibido del Master: %s", mensaje);
        }

        free(mensaje);
    }
}

int socket_master = crear_conexion(query_configs.ipmaster, string_itoa(query_configs.puertomaster));
    log_info(logger_query, "## Conexión al Master exitosa. IP: %s, Puerto: %d", query_configs.ipmaster, query_configs.puertomaster);

    //Serializo el path de la query y la prioridad
    t_query query = {path_query, prioridad};
    t_buffer* buffer = serializar_query(&query);
    t_paquete* paquete = empaquetar_buffer(PAQUETE_QUERY, buffer);

    //Se lo envío a Master
    enviar_paquete(socket_master, paquete);
    log_info(logger_query, "## Solicitud de ejecución de Query: %s, prioridad: %d", path_query, prioridad);



    //Queda a la espera de los mensajes de Master
    while(1){
        printf("entra al while");
        t_paquete* mensaje = recibir_paquete(socket_master);
        switch(mensaje->codigo_operacion){
            case READ:
                //Información leída por la query recibida
                t_operacion_query* informacion = deserializar_operacion_query(mensaje->buffer);
                log_info(logger_query, "## Lectura realizada: File %s:%s, contenido: %s", informacion->file, informacion->tag, informacion->informacion);
                break;

            case END:
                //Finalización de la query recibida
                log_info(logger_query, "## Query Finalizada - Fin de Query");
                close(socket_master);
                return 0;
        }
    }
 
    return 0;
}
