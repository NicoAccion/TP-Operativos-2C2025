#include <utils/hello.h>
#include "master-configs.h"

int main(int argc, char* argv[]) {
    saludar("master");
    inicializar_configs();
    int master_server = iniciar_servidor(int_a_string(master_configs.puertoescucha));
    int master_cliente = esperar_cliente(master_server);

    char* mensaje = recibir_mensaje(master_cliente);
        if (mensaje != NULL) {
        printf("Servidor recibió: %s\n", mensaje);
        free(mensaje);
    }
    enviar_mensaje("Hola desde el MASTER!", master_cliente);

    close(master_cliente);
    close(master_server);
    return 0;
}
