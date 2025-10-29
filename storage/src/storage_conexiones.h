#ifndef STORAGE_CONEXIONES_H
#define STORAGE_CONEXIONES_H

#include <utils/sockets.h>
#include <utils/serializacion.h>
#include "storage-configs.h"
#include "storage-log.h"
#include "storage_operaciones.h"
#include <unistd.h> // <-- Para link() y unlink()

/**
 * @brief Gestiona el ciclo de vida completo de una conexión de un Worker.
 * (Esta es la función que se ejecuta en un hilo separado).
 * @param arg Puntero al socket del cliente (int*).
 */
void* gestionar_conexion_worker(void* arg);

#endif