#ifndef QUERY_LOG_H_
#define QUERY_LOG_H_

#include <commons/log.h>
#include <stdlib.h>

 // Logger global que sera usado en todo el modulo
 extern t_log* logger_query;

/**
* @brief Inicializa un logger que muestra en consola
*/
void inicializar_logger_query();

 /**
  * @brief Destruye el logger del módulo de query
  */
 void destruir_logger();

#endif