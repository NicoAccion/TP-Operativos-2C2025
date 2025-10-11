#ifndef STORAGE_FILESYSTEM_H
#define STORAGE_FILESYSTEM_H

#include <stdbool.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h> 
#include <errno.h>
#include "storage-configs.h"

int fs_crear_archivo(char* nombre_file);
void fs_marcar_bloque_como_usado(int nro_bloque_fisico);
void fs_escribir_bloque(int nro_bloque_fisico, char* contenido, int tamanio);
bool fs_crear_tag(char* nombre_file, char* nombre_tag);

/**
 * @brief Crea un directorio en la ruta especificada.
 *
 * @param dir_name El nombre del nuevo directorio a crear.
 *
 * Esta función construye la ruta completa 'punto_montaje/dir_name' e intenta crearla.
 * Si el directorio ya existe (error EEXIST), lo informa como una advertencia pero no
 * falla. Para cualquier otro error, se registra como un error crítico.
 */
void fs_crear_directorio(const char* dir_name);

#endif 