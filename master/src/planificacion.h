#ifndef PLANIFICACION_H
#define PLANIFICACION_H

#include "atender-hilos.h"

void* planificar();

void fifo();

void prioridades();

bool esta_libre(void* arg);

#endif