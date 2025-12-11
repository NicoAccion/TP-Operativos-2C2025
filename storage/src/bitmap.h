#ifndef BITMAP_H
#define BITMAP_H

#include <commons/bitarray.h>
#include <stdbool.h>
#include <stdio.h>
#include <pthread.h>

typedef struct {
    t_bitarray* bitarray;
    void* bitarray_data;     // Puntero al inicio de la memoria mapeada (mmap)
    int cantidad_bloques;
    int fd_bitmap;       // Usamos File Descriptor para mmap
    size_t size_bytes;   // Tamaño en bytes para munmap
    pthread_mutex_t mutex; // MUTEX 
} t_bitmap_storage;

// Instancia global del bitmap (única en el módulo Storage)
extern t_bitmap_storage bitmap_storage;

// Inicialización y destrucción
void inicializar_bitmap(const char* path_bitmap, int cantidad_bloques, bool limpiar);
void destruir_bitmap(void);

// Operaciones básicas
int reservar_bloque_libre(void);
int buscar_bloque_libre(void);
void marcar_bloque_ocupado(int bloque);
void liberar_bloque(int bloque);
bool bloque_esta_ocupado(int bloque);

// Guardar cambios en disco
void sincronizar_bitmap(void);


void imprimir_bitmap_estado(void);
#endif