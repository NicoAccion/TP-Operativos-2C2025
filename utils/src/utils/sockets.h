#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/types.h>

/**
* @brief Crea un socket para el servidor. Lo pone en modo escucha
* @param puerto el puerto donde va a escuchar el socket del servidor
* @return Devuelve el descriptor del socket
*/
int iniciar_servidor(const char* puerto);

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                Funciones de conexión

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
* @brief Obtiene informacion de la ip, crea un socket, lo conecta con el 
* servidor
* @param ip la ip del server
* @param puerto puerto donde escucha el servidor
* @return Devuelve el descriptor del socket conectado, o valor negativo
* si hay error.
*/
int crear_conexion(const char* ip, const char* puerto);

/**
* @brief Recibe el socket del servidor y el logger del modulo
* @param socket_servidor el socket del servidor que va a aceptar conexiones
* @param logger el logger del modulo
* @return devuelve un socket conectado al socket del servidor
*/
int esperar_cliente(int socket_servidor);

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                Funciones de envio y recepcion de mensajes

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
 * @brief Envia un mensaje string a un socket
 * 
 * @param mensaje el mensaje a enviar
 * @param socket el socket de destino
 */
void enviar_mensaje(const char* mensaje, int socket);

/**
* @brief Recibe el mensaje desde el socket
* @param socket el socket desde donde recibira el mensaje
* @param logger el logger del modulo
* @return devuelve el mensaje recibido char*
*/
char* recibir_mensaje(int socket);

/**
* @brief Convierte un int a string. Basicamente porque cargamos los puertos como int, pero para crear el socket
* necesitamos un string.
* @param numero el numero a convertir
* @return devuelve el numero convertido a string
*/
const char* int_a_string(int numero);