#ifndef WORKER_LOG_H_
#define WORKER_LOG_H_

#include <commons/log.h>
#include <stdlib.h>

 // Logger global que sera usado en todo el modulo
 extern t_log* logger_worker;

/**
* @brief Inicializa un logger que muestra en consola
* @param loglevel Nivel de detalle máximo a mostrar
*/
void inicializar_logger_worker(char* loglevel);

 /**
  * @brief Destruye el logger del módulo de worker.
  */
 void destruir_logger();

#endif