#ifndef QUERY_H_
#define QUERY_H_

#include <stdint.h>
#include <stdlib.h>

#include <commons/string.h>

#include <utils/serializacion.h>
#include "query-configs.h"
#include "query-log.h"

/*//////////////////////////////////////////////////////////////////////////////////////////////////

                                        Prototipos de funciones

//////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
 * @brief Hilo encargado de recibir los mensajes del master
 * 
 * @param socket_master: Socket de la conexión con master
 * @return No devuelve nada
 */
void escuchar_master(int socket_master);

#endif