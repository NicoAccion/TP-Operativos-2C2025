#include "fresh_start.h"
#include "bitmap.h"


void inicializar_initial_file() {
    
    printf("\nInicializando archivos...\n");

    // 1. Creo directorios
    fs_crear_directorio("files/initial_file");
    fs_crear_directorio("files/initial_file/BASE");
    fs_crear_directorio("files/initial_file/BASE/logical_blocks");
    
    // 2. Creo metadata inicial
    metadataconfigs* metadata_inicial = malloc(sizeof(metadataconfigs));
    metadata_inicial->tamanio = 128;
    metadata_inicial->estado = "WORK_IN_PROGRESS";
    metadata_inicial->blocks = list_create();

    uint32_t* bloque0 = malloc(sizeof(uint32_t)); *bloque0 = 0;
    list_add(metadata_inicial->blocks, bloque0);

    // 3. Guardo la estructura en el archivo
    guardar_metadata_en_archivo(metadata_inicial, "initial_file/BASE/metadata.config");

    // 4. Liberar toda la memoria al final
    list_destroy_and_destroy_elements(metadata_inicial->blocks, free);
    free(metadata_inicial);

    printf("Inicializacion de archivos completada.\n");
}


void borrar_datos_existentes() {
    printf("\nIniciando limpieza de persistencia...\n");

    // Buffer para construir la ruta completa dinámicamente
    char ruta_completa[128]; 
    
    // --- Borrado de archivos de metadatos ---
    const char *nombres_archivos[] = {"bitmap.bin", "blocks_hash_index.config"};
    for (int i = 0; i < 2; i++) {
        snprintf(ruta_completa, sizeof(ruta_completa), "%s/%s", 
                 storage_configs.puntomontaje, nombres_archivos[i]);
   
        if (remove(ruta_completa) != 0) {
            // Ignorar el error si es que el archivo no existe
            if (errno != ENOENT) {
                perror("ERROR CRITICO al borrar archivo de persistencia");
            }
        }
    }

    // --- Borrado y Creación de directorios ---
    char comando_limpieza[256];
    const char *nombres_dirs[] = {"physical_blocks", "files"};
    for (int i = 0; i < 2; i++) {
        
        snprintf(ruta_completa, sizeof(ruta_completa), "%s/%s", 
                 storage_configs.puntomontaje, nombres_dirs[i]);
        
        // ... Lógica de borrado y logs ...
        snprintf(comando_limpieza, sizeof(comando_limpieza), "rm -rf %s", ruta_completa);
        system(comando_limpieza);
       
        // ... Lógica de creación y logs ...
        if (mkdir(ruta_completa, 0777) == 0) {
            printf("Directorio creado exitosamente: '%s'\n", ruta_completa);
        } else {
            printf("Hubo un error creando el directorio: %s\n", ruta_completa);
        }
    }

    printf("Limpieza de directorios completada.\n");
}

void crear_blocks_fisicos(){
    uint32_t cantidad = superblock_configs.fssize / superblock_configs.blocksize;
    uint32_t tamanio = superblock_configs.blocksize;

    printf("\nCreando %u archivos de bloque de %u bytes cada uno...\n", cantidad, tamanio);
    for (uint32_t i = 0; i < cantidad; i++) {
        char ruta_bloque[256];
        // El formato "%04d" asegura los ceros a la izquierda (0000, 0001, etc.)
        snprintf(ruta_bloque, sizeof(ruta_bloque), "%s/physical_blocks/block%04d.dat", storage_configs.puntomontaje, i);

        FILE* f_bloque = fopen(ruta_bloque, "w");
        if (f_bloque == NULL) {
            fprintf(stderr, "ERROR FATAL: No se pudo crear el archivo de bloque %s\n", ruta_bloque);
            exit(EXIT_FAILURE);
        }

        // ftruncate es la forma más eficiente de asignar un tamaño a un archivo vacío
        if (ftruncate(fileno(f_bloque), tamanio) != 0) {
            fprintf(stderr, "ERROR FATAL: No se pudo asignar el tamaño al bloque %s\n", ruta_bloque);
            exit(EXIT_FAILURE);
        }

        fclose(f_bloque);
    }
    printf("Archivos de bloque físicos creados.\n");    
}

void crear_bloque_logico_como_link(const char* path_tag, int nro_bloque_fisico, int nro_bloque_logico) {
    // 1. Construir la ruta al bloque FÍSICO (el origen del link)
    char ruta_fisica[1024];
    snprintf(ruta_fisica, sizeof(ruta_fisica), 
             "%s/physical_blocks/block%04d.dat", 
             storage_configs.puntomontaje, 
             nro_bloque_fisico);

    // 2. Construir la ruta al bloque LÓGICO (el destino del link)
    char ruta_logica[1024];
    snprintf(ruta_logica, sizeof(ruta_logica), 
             "%s/%s/logical_blocks/%06d.dat", 
             storage_configs.puntomontaje, 
             path_tag, 
             nro_bloque_logico);

    printf("\nCreando hard link desde '%s' hacia '%s'\n", ruta_fisica, ruta_logica);

    // 3. Crear el hard link
    // La función link() crea un nuevo nombre (ruta_logica) que apunta
    // a los mismos datos en disco que ruta_fisica.
    if (link(ruta_fisica, ruta_logica) != 0) {
        perror("ERROR FATAL: No se pudo crear el hard link del bloque lógico");
        exit(EXIT_FAILURE);
    }

    printf("ÉXITO: Bloque lógico %06d.dat creado como vínculo al bloque físico %d.\n\n", nro_bloque_logico, nro_bloque_fisico);
}

void crear_archivo_bitmap() {
    uint32_t cantidad_bloques = superblock_configs.fssize / superblock_configs.blocksize;
    char ruta_bitmap[256];
    snprintf(ruta_bitmap, sizeof(ruta_bitmap), "%s/bitmap.bin", storage_configs.puntomontaje);

    if (access(ruta_bitmap, F_OK) == 0) {
        log_info(logger_storage, "Bitmap ya existente, no se crea uno nuevo.");
        return;
    }

     FILE* f_bitmap = fopen(ruta_bitmap, "wb");
    if (f_bitmap == NULL) {
        perror("ERROR FATAL: No se pudo crear el bitmap.bin");
        exit(EXIT_FAILURE);
    }

    // Calculamos el tamaño del bitmap en bytes.
    // (cantidad + 7) / 8 es un truco para redondear hacia arriba la división entera.
    size_t tamanio_bitmap = (cantidad_bloques + 7) / 8;

    char* buffer = calloc(1, tamanio_bitmap);
    fwrite(buffer, tamanio_bitmap, 1, f_bitmap);
    free(buffer);
    
    // Lo inicializamos con todos los bits en 0 (todos los bloques libres)
    /*for (size_t i = 0; i < tamanio_bitmap; i++) {
        fputc('\0', f_bitmap);
    }
    */
    
    fclose(f_bitmap);
    log_info(logger_storage, "Archivo bitmap.bin creado e inicializado con %u bloques.", cantidad_bloques);
    inicializar_bitmap(ruta_bitmap, cantidad_bloques);

}

void crear_archivo_hash_index() {
    char ruta_hash[256];
    snprintf(ruta_hash, sizeof(ruta_hash), "%s/blocks_hash_index.config", storage_configs.puntomontaje);

    FILE* f_hash = fopen(ruta_hash, "w");
    if (f_hash == NULL) {
        perror("ERROR FATAL: No se pudo crear el archivo de hash index");
        exit(EXIT_FAILURE);
    }
    fclose(f_hash);
    printf("ÉXITO: Archivo de hash index creado.\n");
}

void inicializar_fs(){

    char ruta_bitmap[256];
    snprintf(ruta_bitmap, sizeof(ruta_bitmap), "%s/bitmap.bin", storage_configs.puntomontaje);

    uint32_t cantidad_bloques = superblock_configs.fssize / superblock_configs.blocksize;


    // Si es un fresh start borro todo lo que hay en el puntomontaje
    if(storage_configs.freshstart){
        printf("\nModo de inicio: FRESH START\n");
        borrar_datos_existentes();
        crear_blocks_fisicos();
        crear_archivo_bitmap();
        crear_archivo_hash_index();
        inicializar_initial_file();
        crear_bloque_logico_como_link("files/initial_file/BASE", 0, 0);
    }
    else {
        printf("Modo de inicio: Normal\n");

        if (access(ruta_bitmap, F_OK) != 0) {
            fprintf(stderr, "ERROR: No se encontró bitmap.bin en %s\n", storage_configs.puntomontaje);
            exit(EXIT_FAILURE);
        }
    }

    inicializar_bitmap(ruta_bitmap, cantidad_bloques);
    log_info(logger_storage, "## Bitmap inicializado en memoria con %u bloques (%u bytes)", 
             cantidad_bloques, (cantidad_bloques + 7) / 8);

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