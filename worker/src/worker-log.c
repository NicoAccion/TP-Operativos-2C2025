#include "worker-log.h"

 // Definición de la variable global logger_worker
 t_log* logger_worker;

void inicializar_logger_worker(char* loglevel) {
    
	logger_worker = log_create("worker.log", "WORKER", true, log_level_from_string(loglevel));
    if (logger_worker == NULL) {
        fprintf(stderr, "Error fatal: No se pudo crear el logger para Worker");
        exit(EXIT_FAILURE);
    }
}

void destruir_logger() {
     // Chequeo que el logger exista y lo destruyo
     if (logger_worker != NULL) {
         log_destroy(logger_worker);
         logger_worker = NULL; // Para evitar doble liberación
     }
}