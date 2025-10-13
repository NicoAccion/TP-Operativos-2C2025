#ifndef MASTER_H_
#define MASTER_H_

#include <commons/string.h>
#include <pthread.h>

#include "master-configs.h"
#include "master-log.h"
#include "planificacion.h"
#include "atender-hilos.h"

void* servidor_general();

#endif