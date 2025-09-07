#include "sockets.h"

// Funciones auxiliares
const char* int_a_string(int numero) {
    static char buffer[6];
    sprintf(buffer, "%d", numero);
    return buffer;
}

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                Funciones de conexión

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

int iniciar_servidor(const char* puerto) {
    int socket_servidor;
    struct addrinfo hints, *server_info;

    //Seteo hints en 0
    memset(&hints,0, sizeof(hints));
    hints.ai_family = AF_INET; //IPV4
    hints.ai_socktype = SOCK_STREAM; //TCP
    hints.ai_flags = AI_PASSIVE; 

    //Obtengo info del puerto e ip local, manejo errores
    int err = getaddrinfo(NULL, puerto, &hints, &server_info);
    if(err !=0){
        fprintf(stderr, "Fallo getaddrinfo - %s\n", gai_strerror(err));
    }

    //Creo el socket, chequeo errores
    socket_servidor = socket(server_info->ai_family,
                           server_info->ai_socktype,
                           server_info->ai_protocol);

    if (socket_servidor == -1){
        fprintf(stderr, "Error al crear el socket: %s\n", strerror(errno));
        freeaddrinfo(server_info);
        return -1;
    }
    
    //Para que pueda reusar puertos, mencionado en tp0
    setsockopt(socket_servidor, SOL_SOCKET,SO_REUSEPORT,&(int){1}, sizeof(int));

    //Bindeo el socket al puerto
    if(bind(socket_servidor,server_info->ai_addr,server_info->ai_addrlen) == -1) {
        fprintf(stderr, "Error en el bind: %s\n", strerror(errno));
        close(socket_servidor);
        freeaddrinfo(server_info);
        return -1;
    }

    //Escucho conexiones entrantes
    if(listen(socket_servidor, SOMAXCONN) == -1) {
        fprintf(stderr, "Error en listen: %s\n", strerror(errno));
        close(socket_servidor);
        freeaddrinfo(server_info);
        return -1;
    }

    //Libero memoria
    freeaddrinfo(server_info);
    
    //Loggeo y devuelvo socket
    printf("Servidor escuchando en el puerto %s !\n", puerto);
    return socket_servidor;
}

int crear_conexion(const char* ip, const char* puerto) {
    struct addrinfo hints, *server_info;

    //Seteo hints en 0
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET; //IPV4
    hints.ai_socktype = SOCK_STREAM; //TCP
    hints.ai_flags = AI_PASSIVE;

    //Obtengo informacion de la IP, puerto y chequeo errores
    int err = getaddrinfo(ip, puerto, &hints, &server_info);
    if(err !=0){
        fprintf(stderr, "Fallo getaddrinfo al conectar con %s:%s - %s\n", ip, puerto, gai_strerror(err));
    }

    //Creo el socket
    int socket_cliente = socket(server_info->ai_family, 
                              server_info->ai_socktype, 
                              server_info->ai_protocol);

    //Chequeo por errores en la creacion
    if(socket_cliente == -1) {
        fprintf(stderr,"Error al crear socket %s\n", strerror(errno));
        freeaddrinfo(server_info);
        return -1;
    }

    //Se conecta al socket con el server, el cual debe estar escuchando. 
    //Chequeo errores              
    if(connect(socket_cliente, server_info->ai_addr, server_info->ai_addrlen) == -1){
        fprintf(stderr,"Fallo al conectar con %s:%s - %s\n", ip, puerto, strerror(errno));
        freeaddrinfo(server_info);
        close(socket_cliente);
        return -1;
    }
    
    printf("Conectado con exito a %s puerto %s\n", ip, puerto);

    //Libero memoria 
    freeaddrinfo(server_info);

    //Devuelvo descriptor del socket conectado
    return socket_cliente;
}

int esperar_cliente(int socket_servidor) {
    //Acepta conexion, chequeo errores
    int socket_cliente = accept(socket_servidor, NULL, NULL);
    if(socket_cliente == -1){
        fprintf(stderr, "Error en el accept(): %s\n", strerror(errno));
        return -1;
    }

    printf("Se conecto un cliente!\n");

    //Devuelvo socket ya conectado con el servidor
    return socket_cliente;
}

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                Funciones de envio y recepcion de mensajes

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

void enviar_mensaje(const char* mensaje, int socket) {
    if(mensaje == NULL){
        return;
    }
    //Tamanio del string +1 para el /0
    int size = strlen(mensaje) + 1;
    //Envio tamanio del string y chequeo errores
    if(send(socket, &size, sizeof(int), 0) == -1){
        fprintf(stderr, "Error al enviar tamanio del mensaje: %s\n", strerror(errno));
        return;
    } 
    //Envio mensaje y chequeo errores
    if (send(socket, mensaje, size, 0) == -1){
        fprintf(stderr, "Error al enviar mensaje: %s\n", strerror(errno));
        return;
    }
}

char* recibir_mensaje(int socket) {
    int size;
    int bytes_recibidos;

    //Recibo tamanio del mensaje a recibir, chequeo errores
    bytes_recibidos = recv(socket, &size, sizeof(int), MSG_WAITALL);
    if(bytes_recibidos <= 0){
        if (bytes_recibidos == -1) {
            fprintf(stderr, "Error al recibir tamanio del mensaje: %s\n", strerror(errno));
        }
        return NULL;
    }
    //Reservo espacio para recibir el mensaje
    char* buffer = malloc(size);
    if(buffer == NULL){
        fprintf(stderr, "Error al reservar memoria para el mensaje: %s\n", strerror(errno));
        return NULL;
    }
    //Recibo mensaje, chequeo errores
    bytes_recibidos = recv(socket, buffer, size, MSG_WAITALL);
    if(bytes_recibidos <= 0){
        if (bytes_recibidos == -1) {
            fprintf(stderr, "Error al recibir el mensaje: %s\n", strerror(errno));
        }
        //Libero memoria reservada para buffer antes de volver
        free(buffer);
        return NULL;
    }
    buffer[size - 1] = '\0';
    return buffer;
}
