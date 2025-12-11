#include "bitmap.h"
#include "storage-log.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <errno.h>

// Instancia global
t_bitmap_storage bitmap_storage = {0};

void inicializar_bitmap(const char* path_bitmap, int cantidad_bloques, bool limpiar) {
    bitmap_storage.cantidad_bloques = cantidad_bloques;
    // Calculamos bytes necesarios: (bloques + 7) / 8
    bitmap_storage.size_bytes = (size_t)(cantidad_bloques + 7) / 8;

    // 1. Inicializar Mutex para concurrencia
    if (pthread_mutex_init(&bitmap_storage.mutex, NULL) != 0) {
        log_error(logger_storage, "Fallo al inicializar mutex de bitmap");
        exit(EXIT_FAILURE);
    }

    // 2. Abrir archivo (Crear si no existe, Lectura/Escritura)
    // 0664 = rw-rw-r--
    bitmap_storage.fd_bitmap = open(path_bitmap, O_CREAT | O_RDWR, 0664);
    if (bitmap_storage.fd_bitmap == -1) {
        log_error(logger_storage, "Error abriendo bitmap.bin: %s", strerror(errno));
        exit(EXIT_FAILURE);
    }

    // 3. Ajustar tamaño del archivo (truncate)
    // Esto asegura que el archivo tenga exactamente el tamaño necesario en bytes
    if (ftruncate(bitmap_storage.fd_bitmap, bitmap_storage.size_bytes) == -1) {
        log_error(logger_storage, "Error truncando bitmap.bin: %s", strerror(errno));
        close(bitmap_storage.fd_bitmap);
        exit(EXIT_FAILURE);
    }

    // 4. Mapear archivo a memoria (MMAP)
    // MAP_SHARED es vital para que los cambios se guarden en el archivo
    bitmap_storage.bitarray_data = mmap(NULL, 
                                        bitmap_storage.size_bytes, 
                                        PROT_READ | PROT_WRITE, 
                                        MAP_SHARED, 
                                        bitmap_storage.fd_bitmap, 
                                        0);

    if (bitmap_storage.bitarray_data == MAP_FAILED) {
        log_error(logger_storage, "Error en mmap de bitmap: %s", strerror(errno));
        close(bitmap_storage.fd_bitmap);
        exit(EXIT_FAILURE);
    }

    // 5. Si es FRESH_START o se solicitó limpiar, llenamos de ceros
    if (limpiar) {
        memset(bitmap_storage.bitarray_data, 0, bitmap_storage.size_bytes);
        // Sincronizamos forzosamente para asegurar que el disco esté limpio
        msync(bitmap_storage.bitarray_data, bitmap_storage.size_bytes, MS_SYNC);
        log_info(logger_storage, "Bitmap limpiado (FRESH_START).");
    }

    // 6. Crear el bitarray de las commons sobre la memoria mapeada
    bitmap_storage.bitarray = bitarray_create_with_mode(
        (char*)bitmap_storage.bitarray_data, 
        bitmap_storage.size_bytes, 
        LSB_FIRST
    );

    log_info(logger_storage, "Bitmap inicializado correctamente: %d bloques, %ld bytes.", 
             cantidad_bloques, bitmap_storage.size_bytes);
}

void destruir_bitmap(void) {
    // 1. Sincronizar cambios pendientes al disco
    if (bitmap_storage.bitarray_data && bitmap_storage.bitarray_data != MAP_FAILED) {
        msync(bitmap_storage.bitarray_data, bitmap_storage.size_bytes, MS_SYNC);
        munmap(bitmap_storage.bitarray_data, bitmap_storage.size_bytes);
    }

    // 2. Liberar estructura de commons
    if (bitmap_storage.bitarray) {
        bitarray_destroy(bitmap_storage.bitarray);
    }

    // 3. Cerrar archivo
    if (bitmap_storage.fd_bitmap != -1) {
        close(bitmap_storage.fd_bitmap);
    }

    // 4. Destruir mutex
    pthread_mutex_destroy(&bitmap_storage.mutex);

    log_info(logger_storage, "Bitmap cerrado y recursos liberados.");
}

int reservar_bloque_libre(void) {
    int bloque_encontrado = -1;

    // Bloqueamos mutex para operación atómica
    pthread_mutex_lock(&bitmap_storage.mutex);

    // Usamos iteración manual o bitarray_test_bit
    for (int i = 0; i < bitmap_storage.cantidad_bloques; i++) {
        if (!bitarray_test_bit(bitmap_storage.bitarray, i)) {
            // Se encuentra uno libre
            bitarray_set_bit(bitmap_storage.bitarray, i); // Lo marcamos OCUPADO ahora mismo
            bloque_encontrado = i;
            break; // Salimos del for
        }
    }

    pthread_mutex_unlock(&bitmap_storage.mutex);

    if (bloque_encontrado != -1) {
        // log_debug(logger_storage, "Bitmap: Bloque %d reservado.", bloque_encontrado);
        return bloque_encontrado;
    } else {
        log_warning(logger_storage, "Bitmap: No hay bloques libres disponibles.");
        return -1; // Espacio insuficiente
    }
}

void liberar_bloque(int bloque) {
    // Validaciones de rango
    if (bloque < 0 || bloque >= bitmap_storage.cantidad_bloques) {
        log_error(logger_storage, "Intento de liberar bloque inválido: %d", bloque);
        return;
    }

    pthread_mutex_lock(&bitmap_storage.mutex);
    
    // Limpiamos el bit
    bitarray_clean_bit(bitmap_storage.bitarray, bloque);
    
    // msync(bitmap_storage.bitarray_data, bitmap_storage.size_bytes, MS_ASYNC); 

    pthread_mutex_unlock(&bitmap_storage.mutex);

    log_debug(logger_storage, "Bitmap: Bloque %d liberado.", bloque);
}

bool bloque_esta_ocupado(int bloque) {
    if (bloque < 0 || bloque >= bitmap_storage.cantidad_bloques) return false;

    bool ocupado;
    pthread_mutex_lock(&bitmap_storage.mutex);
    ocupado = bitarray_test_bit(bitmap_storage.bitarray, bloque);
    pthread_mutex_unlock(&bitmap_storage.mutex);

    return ocupado;
}