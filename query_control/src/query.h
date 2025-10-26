#ifndef QUERY_H_
#define QUERY_H_

#include <stdint.h>

#include <commons/string.h>

#include <utils/hello.h>
#include <utils/serializacion.h>
#include "query-configs.h"
#include "query-log.h"

void escuchar_master(int socket_master);

void destruir_operacion_read(t_operacion_read* op) {
    if (op == NULL) return;

    free(op->informacion);
    free(op->file);
    free(op->tag);
    free(op);
}

#endif