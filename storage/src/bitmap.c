#include "bitmap.h"
#include "storage-log.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

// Instancia global
t_bitmap_storage bitmap_storage = {0};

void inicializar_bitmap(const char* path_bitmap, int cantidad_bloques) {
    bitmap_storage.cantidad_bloques = cantidad_bloques;

    int bytes_necesarios = (cantidad_bloques + 7) / 8; // Redondear a bytes

    // Crear archivo si no existe
    bool nuevo = access(path_bitmap, F_OK) != 0;

    bitmap_storage.archivo_bitmap = fopen(path_bitmap, nuevo ? "wb+" : "rb+");
    if (!bitmap_storage.archivo_bitmap) {
        perror("Error al abrir bitmap.bin");
        exit(EXIT_FAILURE);
    }

    // Si es nuevo, llenarlo con ceros
    if (nuevo) {
        char* buffer_ceros = calloc(1, bytes_necesarios);
        fwrite(buffer_ceros, bytes_necesarios, 1, bitmap_storage.archivo_bitmap);
        free(buffer_ceros);
        log_info(logger_storage, "Archivo bitmap.bin creado (%d bloques, %d bytes).",
                 cantidad_bloques, bytes_necesarios);
    } else {
        log_info(logger_storage, "Archivo bitmap.bin existente encontrado.");
    }

    // Leer contenido a memoria
    rewind(bitmap_storage.archivo_bitmap);
    void* data = malloc(bytes_necesarios);
    fread(data, bytes_necesarios, 1, bitmap_storage.archivo_bitmap);

    bitmap_storage.bitarray = bitarray_create_with_mode(
        data, bytes_necesarios, LSB_FIRST
    );

    log_info(logger_storage, "Bitmap inicializado correctamente con %d bloques.", cantidad_bloques);
}

void destruir_bitmap(void) {
   if (bitmap_storage.bitarray) {
    sincronizar_bitmap();
    if (bitmap_storage.bitarray->bitarray)
        free(bitmap_storage.bitarray->bitarray);
    bitarray_destroy(bitmap_storage.bitarray);
    }

    if (bitmap_storage.archivo_bitmap) {
        fclose(bitmap_storage.archivo_bitmap);
    }

    log_info(logger_storage, "Bitmap liberado correctamente.");
}

void sincronizar_bitmap(void) {
    if (!bitmap_storage.archivo_bitmap || !bitmap_storage.bitarray) return;

    rewind(bitmap_storage.archivo_bitmap);
    fwrite(bitmap_storage.bitarray->bitarray, 
           bitarray_get_max_bit(bitmap_storage.bitarray) / 8 + 1, 
           1, 
           bitmap_storage.archivo_bitmap);
    fflush(bitmap_storage.archivo_bitmap);
}

int buscar_bloque_libre(void) {
    for (int i = 0; i < bitmap_storage.cantidad_bloques; i++) {
        if (!bitarray_test_bit(bitmap_storage.bitarray, i)) {
            log_debug(logger_storage, "Bloque libre encontrado: %d", i);
            return i;
        }
    }
    log_error(logger_storage, "No hay bloques libres disponibles en el FS.");
    return -1;
}

void marcar_bloque_ocupado(int bloque) {
    if (bloque < 0 || bloque >= bitmap_storage.cantidad_bloques) return;
    bitarray_set_bit(bitmap_storage.bitarray, bloque);
    sincronizar_bitmap();
    log_info(logger_storage, "Bloque %d marcado como OCUPADO.", bloque);
}

void liberar_bloque(int bloque) {
    if (bloque < 0 || bloque >= bitmap_storage.cantidad_bloques) return;
    bitarray_clean_bit(bitmap_storage.bitarray, bloque);
    sincronizar_bitmap();
    log_info(logger_storage, "Bloque %d marcado como LIBRE.", bloque);
}

bool bloque_esta_ocupado(int bloque) {
    if (bloque < 0 || bloque >= bitmap_storage.cantidad_bloques) return false;
    return bitarray_test_bit(bitmap_storage.bitarray, bloque);
}

void imprimir_bitmap_estado() {
    printf("Estado del bitmap:\n");
    for (int i = 0; i < bitmap_storage.cantidad_bloques; i++) {
        printf("%d", bitarray_test_bit(bitmap_storage.bitarray, i));
        if ((i + 1) % 8 == 0) printf(" ");
    }
    printf("\n");
}