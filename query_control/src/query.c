#include <utils/hello.h>
#include "query-configs.h"

int main(int argc, char* argv[]) {
    saludar("query_control");
    inicializar_configs();

    int master_server = crear_conexion(query_configs.ipmaster, int_a_string(query_configs.puertomaster));

    enviar_mensaje("Hola desde Query Control!", master_server);

    char* respuesta = recibir_mensaje(master_server);
    printf("Respuesta del master: %s\n", respuesta);
    free(respuesta);

    close(master_server);
    return 0;
}
