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
    uint32_t prioridad = atoi(argv[3]);


    saludar("query_control");

    //Inicializo las configs de Query Control
    inicializar_configs(path_config);

    //Inicializo el logger
    inicializar_logger_query(query_configs.loglevel);

    //Creo la conexión con Master
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