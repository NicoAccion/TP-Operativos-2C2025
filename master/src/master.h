#ifndef MASTER_H_
#define MASTER_H_

#include <commons/string.h>

#include "planificacion.h"
#include "atender-hilos.h"

/*//////////////////////////////////////////////////////////////////////////////////////////////////

                                        Prototipos de funciones

//////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
 * @brief Hilo que escucha las conexiones con el master
 * 
 * @return void*: Un NULL
 */
void* servidor_general(void);

#endif