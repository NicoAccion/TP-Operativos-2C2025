#ifndef QUERY_H_
#define QUERY_H_

#include <stdint.h>

#include <commons/string.h>

#include <utils/hello.h>
#include <utils/serializacion.h>
#include "query-configs.h"
#include "query-log.h"

void escuchar_master(int socket_master);

#endif