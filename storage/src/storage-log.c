#include "storage-log.h"

 // Definición de la variable global logger_storage
 t_log* logger_storage;

void inicializar_logger_storage(char* loglevel) {
    
	logger_storage = log_create("storage.log", "STORAGE", true, log_level_from_string(loglevel));
    if (logger_storage == NULL) {
        fprintf(stderr, "Error fatal: No se pudo crear el logger para Storage");
        exit(EXIT_FAILURE);
    }
}

void destruir_logger() {
     // Chequeo que el logger exista y lo destruyo
     if (logger_storage != NULL) {
         log_destroy(logger_storage);
         logger_storage = NULL; // Para evitar doble liberación
     }
}