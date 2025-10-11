#ifndef WORKER_MEMORIA_H
#define WORKER_MEMORIA_H

#include <stdbool.h>

#define MAX_PAGINAS 1024
#define MAX_FILETAG 128
#define MAX_FRAMES 256


typedef struct {
    int marco;
    bool modificado;
    bool usado;
    unsigned long long timestamp; // Para LRU
    char file[128];
    char tag[128];
    int num_pagina;
    bool ocupado;
} PaginaMemoria;

void inicializar_memoria(int tam_memoria, int tam_pagina);
void escribir_memoria(const char* file, const char* tag, int direccion, const char* contenido);
void leer_memoria(const char* file, const char* tag, int direccion, int tamanio);
void liberar_memoria();
int obtener_marco_libre();
int reemplazar_pagina(); // Retorna índice del marco víctima
void marcar_uso(int marco);

#endif