#include "master-log.h"

 // Definición de la variable global logger_master
 t_log* logger_master;

void inicializar_logger_master(char* loglevel) {
    
	logger_master = log_create("master.log", "MASTER", true, log_level_from_string(loglevel));
    if (logger_master == NULL) {
        fprintf(stderr, "Error fatal: No se pudo crear el logger para Master");
        exit(EXIT_FAILURE);
    }
}

void destruir_logger() {
     // Chequeo que el logger exista y lo destruyo
     if (logger_master != NULL) {
         log_destroy(logger_master);
         logger_master = NULL; // Para evitar doble liberación
     }
}