#ifndef MASTER_LOG_H_
#define MASTER_LOG_H_

#include <commons/log.h>
#include <stdlib.h>

 // Logger global que sera usado en todo el modulo
 extern t_log* logger_master;

/**
* @brief Inicializa un logger que muestra en consola
*/
void inicializar_logger_master();

 /**
  * @brief Destruye el logger del módulo de master
  */
 void destruir_logger();

#endif