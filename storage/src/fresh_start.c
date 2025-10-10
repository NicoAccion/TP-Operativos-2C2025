#include "fresh_start.h"

void borrar_datos_existentes() {
    printf("Iniciando limpieza de persistencia...\n");

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
            printf("Directorio creado exitosamente: %s\n", ruta_completa);
        } else {
            printf("Hubo un error creando el directorio: %s\n", ruta_completa);
        }
    }

    printf("Limpieza y preparación de directorios completada.\n");
}


void inicializar_directorios(){
    // Si es un fresh start borro todo lo que hay en storage
    if(storage_configs.freshstart){
        printf("Modo de inicio: FRESH START\n");
        borrar_datos_existentes();
        return;
    }

    printf("Modo de inicio: Normal\n");
}
