#ifndef STORAGE_H
#define STORAGE_H

#include <stdbool.h>

#define MAX_PATH 1024
#define MAX_LINE 1024

typedef struct {
    int puerto_escucha;
    bool fresh_start;
    char punto_montaje[MAX_PATH];
    int retardo_operacion;       // ms
    int retardo_acceso_bloque;   // ms
    int fs_size;
    int block_size;
    int cantidad_bloques;
} t_storage_config;

void leer_config_storage(const char* path_config, t_storage_config* cfg);

#endif
