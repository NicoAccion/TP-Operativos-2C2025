#ifndef STORAGE_LOG_H_
#define STORAGE_LOG_H_

#include <commons/log.h>
#include <stdlib.h>

 // Logger global que sera usado en todo el modulo
 extern t_log* logger_storage;

/**
* @brief Inicializa un logger que muestra en consola
* @param loglevel Nivel de detalle máximo a mostrar
*/
void inicializar_logger_storage(char* loglevel);

 /**
  * @brief Destruye el logger del módulo de storage.
  */
 void destruir_logger();

#endif