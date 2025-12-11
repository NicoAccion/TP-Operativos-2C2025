#include "fresh_start.h"
#include "bitmap.h"
#include <fcntl.h>
#include <string.h>

// --- FUNCIONES AUXILIARES ---

void fs_crear_directorio(const char* dir_name) {
    char path_completo[512];
    snprintf(path_completo, sizeof(path_completo), "%s/%s", storage_configs.puntomontaje, dir_name);

    if (mkdir(path_completo, 0777) == 0) {
        // Exito
    } else {
        if (errno != EEXIST) {
            log_error(logger_storage, "ERROR creando directorio %s: %s", path_completo, strerror(errno));
        }
    }
}

void inicializar_initial_file() {
    log_info(logger_storage, "Generando estructura 'initial_file'...");

    // 1. Directorios
    fs_crear_directorio("files");
    fs_crear_directorio("files/initial_file");
    fs_crear_directorio("files/initial_file/BASE");
    fs_crear_directorio("files/initial_file/BASE/logical_blocks");
    
    // 2. IMPORTANTE: Reservar bloque 0
    int bloque_reservado = reservar_bloque_libre();
    
    if (bloque_reservado != 0) {
        log_warning(logger_storage, "ALERTA: El bloque reservado para initial_file fue %d (se esperaba 0).", bloque_reservado);
    } else {
        log_info(logger_storage, "Bloque 0 reservado para 'initial_file'.");
    }

    // 3. Metadata
    metadataconfigs* metadata_inicial = malloc(sizeof(metadataconfigs));
    metadata_inicial->tamanio = superblock_configs.blocksize; 
    metadata_inicial->estado = "WORK_IN_PROGRESS";
    metadata_inicial->blocks = list_create();

    uint32_t* bloque0 = malloc(sizeof(uint32_t)); 
    *bloque0 = 0; 
    list_add(metadata_inicial->blocks, bloque0);

    // 3. Guardo la estructura en el archivo
    guardar_metadata_en_archivo(metadata_inicial, "initial_file/BASE/metadata.config");

    // 4. Liberar toda la memoria al final
    list_destroy_and_destroy_elements(metadata_inicial->blocks, free);
    free(metadata_inicial);
}

void borrar_datos_existentes() {
    log_info(logger_storage, "Limpiando persistencia en: %s", storage_configs.puntomontaje);

    char ruta_completa[512]; 
    const char *nombres_archivos[] = {"bitmap.bin", "blocks_hash_index.config"};
    
    for (int i = 0; i < 2; i++) {
        snprintf(ruta_completa, sizeof(ruta_completa), "%s/%s", storage_configs.puntomontaje, nombres_archivos[i]);
        unlink(ruta_completa);
    }

    char comando_limpieza[1024];
    const char *nombres_dirs[] = {"physical_blocks", "files"};
    
    for (int i = 0; i < 2; i++) {
        snprintf(ruta_completa, sizeof(ruta_completa), "%s/%s", storage_configs.puntomontaje, nombres_dirs[i]);
        snprintf(comando_limpieza, sizeof(comando_limpieza), "rm -rf \"%s\"", ruta_completa);
        system(comando_limpieza);

        if (strcmp(nombres_dirs[i], "physical_blocks") == 0) {
             mkdir(ruta_completa, 0777);
        }
    }
}

void crear_blocks_fisicos() {
    uint32_t cantidad = superblock_configs.fssize / superblock_configs.blocksize;
    uint32_t tamanio = superblock_configs.blocksize;

    log_info(logger_storage, "Creando %u archivos de bloque físico...", cantidad);
    char ruta_bloque[512];
    char ruta_padre[512];
    snprintf(ruta_padre, sizeof(ruta_padre), "%s/physical_blocks", storage_configs.puntomontaje);
    mkdir(ruta_padre, 0777);

    for (uint32_t i = 0; i < cantidad; i++) {
        snprintf(ruta_bloque, sizeof(ruta_bloque), "%s/physical_blocks/block%04d.dat", storage_configs.puntomontaje, i);
        int fd = open(ruta_bloque, O_CREAT | O_WRONLY | O_TRUNC, 0664);
        if (fd == -1) exit(EXIT_FAILURE);
        ftruncate(fd, tamanio);
        close(fd);
    }
}

void crear_archivo_hash_index() {
    char ruta_hash[512];
    snprintf(ruta_hash, sizeof(ruta_hash), "%s/blocks_hash_index.config", storage_configs.puntomontaje);
    close(open(ruta_hash, O_CREAT | O_WRONLY | O_TRUNC, 0664));
}

void crear_bloque_logico_como_link(const char* path_tag, int nro_bloque_fisico, int nro_bloque_logico) {
    char ruta_fisica[512];
    char ruta_logica[512];

    snprintf(ruta_fisica, sizeof(ruta_fisica), "%s/physical_blocks/block%04d.dat", storage_configs.puntomontaje, nro_bloque_fisico);
    snprintf(ruta_logica, sizeof(ruta_logica), "%s/files/%s/logical_blocks/%06d.dat", storage_configs.puntomontaje, path_tag, nro_bloque_logico);

    if (link(ruta_fisica, ruta_logica) != 0 && storage_configs.freshstart) exit(EXIT_FAILURE);
}

void inicializar_fs() {
    char ruta_bitmap[256];
    snprintf(ruta_bitmap, sizeof(ruta_bitmap), "%s/bitmap.bin", storage_configs.puntomontaje);
    uint32_t cantidad_bloques = superblock_configs.fssize / superblock_configs.blocksize;

    if (storage_configs.freshstart) {
        log_info(logger_storage, "=== FRESH START ===");
        borrar_datos_existentes();
        crear_blocks_fisicos();
        crear_archivo_hash_index();
        
        // ESTA ES LA CLAVE: inicializar con TRUE. NO llamar a crear_archivo_bitmap
        inicializar_bitmap(ruta_bitmap, cantidad_bloques, true);
        
        inicializar_initial_file();
        crear_bloque_logico_como_link("initial_file/BASE", 0, 0);
    } else {
        log_info(logger_storage, "=== NORMAL START ===");
        if (access(ruta_bitmap, F_OK) != 0) exit(EXIT_FAILURE);
        inicializar_bitmap(ruta_bitmap, cantidad_bloques, false);
    }
}