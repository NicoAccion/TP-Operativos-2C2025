#include "fs_simulacion.h"

int fs_crear_archivo(char* nombre_file) {
    printf("[DUMMY] Llamada a fs_crear_archivo('%s').\n", nombre_file);
    return 0; 
}

void fs_crear_directorio(const char* dir_name) {
    char path_completo[512];

    // 1. Construimos la ruta completa de forma segura
    snprintf(path_completo, sizeof(path_completo), "%s/%s", storage_configs.puntomontaje, dir_name);

    // 2. Intentamos crear el directorio
    int resultado = mkdir(path_completo, 0777);

    if (resultado == 0) {
        printf("Directorio creado exitosamente: '%s' \n", path_completo);
    } else {
        // Error: verificamos la causa
        if (errno == EEXIST) {
            printf("WARNING: El directorio '%s' ya existía.\n", path_completo);
        } else {
            fprintf(stderr, "ERROR: No se pudo crear el directorio '%s'. Motivo: %s\n", path_completo, strerror(errno));
        }
    }
}

void fs_marcar_bloque_como_usado(int nro_bloque_fisico) {
    printf("[DUMMY] Bloque fisico %d marcado como usado.\n", nro_bloque_fisico);
}

void fs_escribir_bloque(int nro_bloque_fisico, char* contenido, int tamanio) {
    printf("[DUMMY] Escribiendo %d bytes en Bloque %d.\n", tamanio, nro_bloque_fisico);
}

bool fs_crear_tag(char* nombre_file, char* nombre_tag) {
    printf("[DUMMY] Creando Tag '%s' para el File '%s'.\n", nombre_tag, nombre_file);
    return true;
}