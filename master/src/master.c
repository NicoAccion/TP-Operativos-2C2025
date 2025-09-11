#include "master.h"

int main(int argc, char* argv[]) {
    saludar("master");

    //Inicializo las configs de Master
    inicializar_configs();

    //Inicializo el logger
    inicializar_logger_master(master_configs.loglevel);

    //Inicio el servidor
    int master_server = iniciar_servidor(string_itoa(master_configs.puertoescucha));

    //Espero la conexión de Query Control
    int socket_querycontrol = esperar_cliente(master_server);

    //Recibo el Path del query que me envió Query Control
    char* path_query = recibir_mensaje(socket_querycontrol);
        if (path_query != NULL) {
        printf("Path del Query que recibí de Query Control: %s\n", path_query);
    }

    close(socket_querycontrol);

    //Espero la conexión de Worker
    int socket_worker = esperar_cliente(master_server);

    //Envío el Path de la query que me pasó Query Control a Worker
    printf("Envío a Worker el Path: %s\n", path_query);
    enviar_mensaje(path_query, socket_worker);

    close(socket_worker);
    close(master_server);
    return 0;
}
