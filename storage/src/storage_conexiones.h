#ifndef STORAGE_CONEXIONES_H
#define STORAGE_CONEXIONES_H

// Incluimos todo lo que la función `atender_worker` necesita
#include <utils/sockets.h>
#include <utils/serializacion.h>
#include "storage-configs.h"
#include "storage-log.h"
#include "storage_operaciones.h"

/**
 * @brief Gestiona el ciclo de vida completo de una conexión de un Worker.
 * (Esta es la función que se ejecuta en un hilo separado).
 * @param arg Puntero al socket del cliente (int*).
 */
void* gestionar_conexion_worker(void* arg);

#endif