#ifndef BITMAP_H
#define BITMAP_H

#include <commons/bitarray.h>
#include <stdbool.h>
#include <stdio.h>

typedef struct {
    t_bitarray* bitarray;
    int cantidad_bloques;
    FILE* archivo_bitmap;
} t_bitmap_storage;

// Instancia global del bitmap (única en el módulo Storage)
extern t_bitmap_storage bitmap_storage;

// Inicialización y destrucción
void inicializar_bitmap(const char* path_bitmap, int cantidad_bloques);
void destruir_bitmap(void);

// Operaciones básicas
int buscar_bloque_libre(void);
void marcar_bloque_ocupado(int bloque);
void liberar_bloque(int bloque);
bool bloque_esta_ocupado(int bloque);

// Guardar cambios en disco
void sincronizar_bitmap(void);


void imprimir_bitmap_estado(void);
#endif