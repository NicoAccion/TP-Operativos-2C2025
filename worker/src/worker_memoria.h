#ifndef WORKER_MEMORIA_H
#define WORKER_MEMORIA_H

#include <stdbool.h>
#include <stdint.h>
#include <utils/serializacion.h>

#define MAX_FILETAG 128

typedef struct {
    bool ocupado;
    bool modificado;
    bool usado;
    unsigned long long timestamp; // Para LRU
    char file[MAX_FILETAG];
    char tag[MAX_FILETAG];
    int num_pagina;
    bool bit_clock; // Para CLOCK-M
} PaginaMemoria;

void inicializar_memoria(int tam_memoria, int tam_pagina);
void liberar_memoria();

// Funciones principales de acceso
void escribir_en_memoria(int query_id, const char* file, const char* tag, int direccion_logica, const char* contenido, int socket_storage, int socket_master);
char* leer_de_memoria(int query_id, const char* file, const char* tag, int direccion_logica, int tamanio, int socket_storage, int socket_master);
void realizar_flush_file(int query_id, const char* file, const char* tag, int socket_storage, int socket_master);

#endif