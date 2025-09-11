#include "query-log.h"

 // Definición de la variable global logger_query
 t_log* logger_query;

void inicializar_logger_query(char* loglevel) {
    
	logger_query = log_create("query.log", "QUERY CONTROL", true, log_level_from_string(loglevel));
    if (logger_query == NULL) {
        fprintf(stderr, "Error fatal: No se pudo crear el logger para Query Control");
        exit(EXIT_FAILURE);
    }
}

void destruir_logger() {
     // Chequeo que el logger exista y lo destruyo
     if (logger_query != NULL) {
         log_destroy(logger_query);
         logger_query = NULL; // Para evitar doble liberación
     }
}