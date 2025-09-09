#include "storage.h"

int main(int argc, char* argv[]) {
    saludar("storage");

    //Inicializo las configs de Master
    inicializar_configs();

    //Inicio el servidor
    int storage_server = iniciar_servidor(string_itoa(storage_configs.puertoescucha));

    //Espero la conexión de Worker
    int socket_worker = esperar_cliente(storage_server);

    //Recibo la solicitud de Worker por el tamaño de bloque
    char* solicitud = recibir_mensaje(socket_worker);
    if (string_equals_ignore_case(solicitud, "Pasame el tamaño del bloque") == 1){
        printf("Recibí la solicitud de parte de Worker\n");
    }

    //Envío el tamaño solicitado a Worker
    printf("Envío a Worker el tamaño del bloque: %s\n", "Qué te importa");
    enviar_mensaje("Qué te importa", socket_worker);

    return 0;
}
