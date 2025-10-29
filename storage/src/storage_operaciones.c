#include "storage_operaciones.h"

/**
 * @brief Función auxiliar para validar si un directorio existe.
 */
bool directorio_existe(char* path) {
    struct stat st = {0};
    
    // Usamos stat() para obtener info del path.
    // Si stat() devuelve -1, el path no existe o hay un error.
    if (stat(path, &st) == -1) {
        return false;
    }

    // Usamos S_ISDIR() sobre el campo st_mode (¡no d_flags!)
    // para verificar si es un directorio.
    return S_ISDIR(st.st_mode); //Valido?
}


t_codigo_operacion storage_op_create(t_op_storage* op) {
    
    // 1. Armamos los paths que vamos a necesitar
    char* path_file = string_from_format("%s/files/%s", storage_configs.puntomontaje, op->nombre_file);
    char* path_tag = string_from_format("%s/%s", path_file, op->nombre_tag);
    char* path_metadata = string_from_format("%s/metadata.config", path_tag);
    char* path_logical_blocks = string_from_format("%s/logical_blocks", path_tag);

    // 2. Validar preexistencia (Error "File / Tag preexistente")
    if (directorio_existe(path_tag)) {
        log_error(logger_storage, "##%d Error: File/Tag preexistente %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
        
        free(path_file);
        free(path_tag);
        free(path_metadata);
        free(path_logical_blocks);
        return OP_ERROR; // Devolvemos error
    }

    // 3. Crear las estructuras de directorios
    mkdir(path_file, 0777); // Crea el dir del File (si no existe)
    mkdir(path_tag, 0777);  // Crea el dir del Tag
    mkdir(path_logical_blocks, 0777); // Crea el dir logical_blocks

    // 4. Crear y escribir el metadata.config inicial
    FILE* f_metadata = fopen(path_metadata, "w");
    if (f_metadata == NULL) {
        log_error(logger_storage, "##%d Error: No se pudo crear metadata.config para %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
        
        free(path_file);
        free(path_tag);
        free(path_metadata);
        free(path_logical_blocks);
        return OP_ERROR;
    }

    fprintf(f_metadata, "TAMAÑO=0\n");
    fprintf(f_metadata, "ESTADO=WORK_IN_PROGRESS\n");
    fprintf(f_metadata, "BLOCKS=[]\n");
    fclose(f_metadata);

    // 5. Loguear éxito (Log obligatorio)
    log_info(logger_storage, "##%d File Creado %s:%s", op->query_id, op->nombre_file, op->nombre_tag);

    // 6. Liberar memoria de los paths y devolver OK
    free(path_file);
    free(path_tag);
    free(path_metadata);
    free(path_logical_blocks);
    
    return OP_OK;
}

t_codigo_operacion storage_op_truncate(t_op_storage* op) {
    
    // 0. Validar que el tamaño sea múltiplo de BLOCK_SIZE
    if (op->tamano % superblock_configs.blocksize != 0) {
        log_error(logger_storage, "##%d Error: Tamaño de TRUNCATE (%d) no es múltiplo de BLOCK_SIZE (%d)",
                  op->query_id, op->tamano, superblock_configs.blocksize);
        return OP_ERROR;
    }

    // 1. Armar paths
    char* path_tag = string_from_format("%s/files/%s/%s", 
                                      storage_configs.puntomontaje, 
                                      op->nombre_file, 
                                      op->nombre_tag);
    char* path_metadata = string_from_format("%s/metadata.config", path_tag);
    char* path_logical_blocks_dir = string_from_format("%s/logical_blocks", path_tag);
    
    // 2. Abrir y leer metadata
    t_config* metadata = config_create(path_metadata);
    if (metadata == NULL) {
        log_error(logger_storage, "##%d Error: No se encontró metadata para %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
        return OP_ERROR; // Error: File / Tag inexistente
    }

    // 3. Chequear estado "COMMITED" 
    char* estado = config_get_string_value(metadata, "ESTADO");
    if (strcmp(estado, "COMMITED") == 0) {
        log_error(logger_storage, "##%d Error: No se puede truncar un File:Tag en estado COMMITED", op->query_id);
        config_destroy(metadata);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
        return OP_ERROR; // Error: Escritura no permitida
    }

    // 4. Calcular bloques
    // int tamano_actual = config_get_int_value(metadata, "TAMAÑO"); // No lo usamos, pero es útil saberlo
    char** bloques_actuales_array = config_get_array_value(metadata, "BLOCKS");
    int bloques_actuales_count = string_array_size(bloques_actuales_array);
    int bloques_nuevos_count = op->tamano / superblock_configs.blocksize;
    int diff = bloques_nuevos_count - bloques_actuales_count;

    char* path_bloque_fisico_0 = string_from_format("%s/physical_blocks/block0000.dat", storage_configs.puntomontaje);

    // 5. Aplicar lógica
    if (diff > 0) {
        // --- AGRANDAR ---
        for (int i = 0; i < diff; i++) {
            int nro_bloque_logico = bloques_actuales_count + i;
            char* nombre_logico = string_from_format("%06d.dat", nro_bloque_logico);
            char* path_bloque_logico = string_from_format("%s/%s", path_logical_blocks_dir, nombre_logico);

            // Crear hard link al bloque físico 0 
            if (link(path_bloque_fisico_0, path_bloque_logico) == -1) {
                 log_error(logger_storage, "Error al crear hard link para bloque lógico %d", nro_bloque_logico);
            } else {
                 log_info(logger_storage, "##%d Hard Link Agregado: %s:%s, Bloque Lógico %d -> Bloque Físico 0",
                          op->query_id, op->nombre_file, op->nombre_tag, nro_bloque_logico);
            }
            free(nombre_logico);
            free(path_bloque_logico);
        }
    } 
    else if (diff < 0) {
        // --- ACHICAR ---
        for (int i = bloques_actuales_count - 1; i >= bloques_nuevos_count; i--) {
            char* nro_bloque_fisico_str = bloques_actuales_array[i];
            char* nombre_logico = string_from_format("%06d.dat", i);
            char* path_bloque_logico = string_from_format("%s/%s", path_logical_blocks_dir, nombre_logico);
            char* path_bloque_fisico = string_from_format("%s/physical_blocks/block%04d.dat", 
                                                         storage_configs.puntomontaje, 
                                                         atoi(nro_bloque_fisico_str));
            
            // Eliminar hard link
            if (unlink(path_bloque_logico) == -1) {
                log_error(logger_storage, "Error al eliminar hard link para bloque lógico %d", i);
            } else {
                 log_info(logger_storage, "##%d Hard Link Eliminado: %s:%s, Bloque Lógico %d (apuntaba a Físico %s)",
                          op->query_id, op->nombre_file, op->nombre_tag, i, nro_bloque_fisico_str);
            }

            // --- Lógica de Bitmap ---
            struct stat st;
            if (stat(path_bloque_fisico, &st) == 0) {
                // Si nlink == 1, solo queda la referencia en /physical_blocks
                // y podemos liberar el bloque (excepto el bloque 0).
                if (st.st_nlink == 1 && atoi(nro_bloque_fisico_str) != 0) {
                    log_info(logger_storage, "Bloque físico %s ya no está referenciado. Liberando...", nro_bloque_fisico_str);
                    
                    // TODO: Implementar lógica de bitarray aquí
                    // bitarray_clean_bit(bitmap, atoi(nro_bloque_fisico_str))
                    
                    log_info(logger_storage, "##%d Bloque Físico Liberado %s", op->query_id, nro_bloque_fisico_str);
                }
            }
            
            free(nombre_logico);
            free(path_bloque_logico);
            free(path_bloque_fisico);
        }
    }

    // 6. Actualizar y guardar metadata 
    char* nuevo_array_str = build_blocks_string(bloques_actuales_array, bloques_actuales_count, bloques_nuevos_count);
    char* tamano_str = string_itoa(op->tamano);

    config_set_value(metadata, "TAMAÑO", tamano_str);
    config_set_value(metadata, "BLOCKS", nuevo_array_str);
    
    config_save(metadata);
    
    log_info(logger_storage, "##%d File Truncado %s:%s Tamaño: %d", 
             op->query_id, op->nombre_file, op->nombre_tag, op->tamano);
    
    // 7. Liberar todo
    free(tamano_str);
    free(nuevo_array_str);
    free(path_bloque_fisico_0);
    string_array_destroy(bloques_actuales_array);
    config_destroy(metadata);
    free(path_tag); 
    free(path_metadata); 
    free(path_logical_blocks_dir);
    
    return OP_OK;
}

t_codigo_operacion storage_op_tag(t_op_storage* op) {

    // 1. Armar Paths de Origen y Destino
    char* path_tag_origen = string_from_format("%s/files/%s/%s", 
                                      storage_configs.puntomontaje, 
                                      op->nombre_file, 
                                      op->nombre_tag);
    char* path_metadata_origen = string_from_format("%s/metadata.config", path_tag_origen);
    
    char* path_file_destino = string_from_format("%s/files/%s", 
                                                 storage_configs.puntomontaje, 
                                                 op->nombre_file_destino);
    char* path_tag_destino = string_from_format("%s/%s", 
                                                path_file_destino, 
                                                op->nombre_tag_destino);
    char* path_metadata_destino = string_from_format("%s/metadata.config", path_tag_destino);
    char* path_logical_blocks_destino = string_from_format("%s/logical_blocks", path_tag_destino);

    // 2. Validar Origen (File / Tag inexistente)
    t_config* metadata_origen = config_create(path_metadata_origen);
    if (metadata_origen == NULL) {
        log_error(logger_storage, "##%d Error: No se encontró File/Tag origen %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
        free(path_tag_origen); free(path_metadata_origen); free(path_file_destino);
        free(path_tag_destino); free(path_metadata_destino); free(path_logical_blocks_destino);
        return OP_ERROR;
    }

    // 3. Validar Destino (File / Tag preexistente)
    if (directorio_existe(path_tag_destino)) {
        log_error(logger_storage, "##%d Error: File/Tag destino %s:%s ya existe", op->query_id, op->nombre_file_destino, op->nombre_tag_destino);
        config_destroy(metadata_origen);
        free(path_tag_origen); free(path_metadata_origen); free(path_file_destino);
        free(path_tag_destino); free(path_metadata_destino); free(path_logical_blocks_destino);
        return OP_ERROR;
    }

    // 4. Crear directorios de Destino
    mkdir(path_file_destino, 0777);
    mkdir(path_tag_destino, 0777);
    mkdir(path_logical_blocks_destino, 0777);

    // 5. Leer datos del metadata origen
    char* tamano_str = config_get_string_value(metadata_origen, "TAMAÑO");
    char* bloques_str = config_get_string_value(metadata_origen, "BLOCKS");
    char** bloques_array = config_get_array_value(metadata_origen, "BLOCKS");
    int num_bloques = string_array_size(bloques_array);

    // 6. Escribir el metadata de Destino (Copia, pero con estado WIP)
    FILE* f_metadata_destino = fopen(path_metadata_destino, "w");
    if (f_metadata_destino == NULL) {
        log_error(logger_storage, "##%d Error: No se pudo crear metadata.config para %s:%s", op->query_id, op->nombre_file_destino, op->nombre_tag_destino);
        // ... (Faltaría cleanup de directorios creados) ...
        config_destroy(metadata_origen);
        string_array_destroy(bloques_array);
        free(path_tag_origen); free(path_metadata_origen); free(path_file_destino);
        free(path_tag_destino); free(path_metadata_destino); free(path_logical_blocks_destino);
        return OP_ERROR;
    }
    
    fprintf(f_metadata_destino, "TAMAÑO=%s\n", tamano_str);
    fprintf(f_metadata_destino, "ESTADO=WORK_IN_PROGRESS\n");
    fprintf(f_metadata_destino, "BLOCKS=%s\n", bloques_str);
    fclose(f_metadata_destino);

    // 7. Replicar los hard links en el directorio de destino
    for (int i = 0; i < num_bloques; i++) {
        char* nro_bloque_fisico_str = bloques_array[i];
        char* nombre_logico_destino = string_from_format("%06d.dat", i);
        char* path_bloque_logico_destino = string_from_format("%s/%s", path_logical_blocks_destino, nombre_logico_destino);
        char* path_bloque_fisico = string_from_format("%s/physical_blocks/block%04d.dat", 
                                                     storage_configs.puntomontaje, 
                                                     atoi(nro_bloque_fisico_str));

        if (link(path_bloque_fisico, path_bloque_logico_destino) == -1) {
            log_error(logger_storage, "Error al replicar hard link para bloque lógico %d (físico %s)", i, nro_bloque_fisico_str);
        } else {
            log_info(logger_storage, "##%d Hard Link Agregado: %s:%s, Bloque Lógico %d -> Bloque Físico %s",
                     op->query_id, op->nombre_file_destino, op->nombre_tag_destino, i, nro_bloque_fisico_str);
        }
        
        free(nombre_logico_destino);
        free(path_bloque_logico_destino);
        free(path_bloque_fisico);
    }

    // 8. Loguear éxito y liberar memoria
    log_info(logger_storage, "##%d Tag creado %s:%s", 
             op->query_id, op->nombre_file_destino, op->nombre_tag_destino);

    config_destroy(metadata_origen);
    string_array_destroy(bloques_array);
    free(path_tag_origen); free(path_metadata_origen); free(path_file_destino);
    free(path_tag_destino); free(path_metadata_destino); free(path_logical_blocks_destino);
    
    return OP_OK;
}

t_codigo_operacion storage_op_delete(t_op_storage* op) {
    
    // 1. Armar Paths
    char* path_file = string_from_format("%s/files/%s", 
                                      storage_configs.puntomontaje, 
                                      op->nombre_file);
    char* path_tag = string_from_format("%s/%s", path_file, op->nombre_tag);
    char* path_metadata = string_from_format("%s/metadata.config", path_tag);
    char* path_logical_blocks_dir = string_from_format("%s/logical_blocks", path_tag);

    // 2. Validar y leer metadata
    t_config* metadata = config_create(path_metadata);
    if (metadata == NULL) {
        log_error(logger_storage, "##%d Error: No se encontró File/Tag %s:%s para eliminar", op->query_id, op->nombre_file, op->nombre_tag);
        free(path_file); free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
        return OP_ERROR; // Error: File / Tag inexistente
    }
    
    // (chequear si está COMMITED?? se debe hacer
    // char* estado = config_get_string_value(metadata, "ESTADO");
    // if (strcmp(estado, "COMMITED") == 0) { ... }

    // 3. Iterar y eliminar hard links + chequear bloques físicos
    char** bloques_array = config_get_array_value(metadata, "BLOCKS");
    int num_bloques = string_array_size(bloques_array);

    for (int i = 0; i < num_bloques; i++) {
        char* nro_bloque_fisico_str = bloques_array[i];
        char* nombre_logico = string_from_format("%06d.dat", i);
        char* path_bloque_logico = string_from_format("%s/%s", path_logical_blocks_dir, nombre_logico);
        char* path_bloque_fisico = string_from_format("%s/physical_blocks/block%04d.dat", 
                                                     storage_configs.puntomontaje, 
                                                     atoi(nro_bloque_fisico_str));

        // 3.a. Eliminar el hard link
        if (unlink(path_bloque_logico) == -1) {
            log_error(logger_storage, "Error al eliminar hard link para bloque lógico %d", i);
        } else {
             log_info(logger_storage, "##%d Hard Link Eliminado: %s:%s, Bloque Lógico %d (apuntaba a Físico %s)",
                      op->query_id, op->nombre_file, op->nombre_tag, i, nro_bloque_fisico_str);
        }

        // 3.b. Chequear 'nlink' del bloque físico
        struct stat st;
        if (stat(path_bloque_fisico, &st) == 0) {
            // Si nlink == 1, solo queda la referencia en /physical_blocks
            // y podemos liberar el bloque (excepto el bloque 0).
            if (st.st_nlink == 1 && atoi(nro_bloque_fisico_str) != 0) {
                log_info(logger_storage, "Bloque físico %s ya no está referenciado. Liberando...", nro_bloque_fisico_str);
                
                // TODO: Implementar lógica de bitarray aquí
                // bitarray_clean_bit(bitmap, atoi(nro_bloque_fisico_str))
                
                log_info(logger_storage, "##%d Bloque Físico Liberado %s", op->query_id, nro_bloque_fisico_str);
            }
        }
        
        free(nombre_logico);
        free(path_bloque_logico);
        free(path_bloque_fisico);
    }

    // 4. Eliminar los archivos y directorios
    string_array_destroy(bloques_array);
    config_destroy(metadata);

    unlink(path_metadata); // Borra metadata.config
    rmdir(path_logical_blocks_dir); // Borra /logical_blocks (debe estar vacío)
    rmdir(path_tag); // Borra /TAG (debe estar vacío)

    // 5. (Opcional) Borrar el directorio del FILE si queda vacío
    DIR* dir = opendir(path_file);
    struct dirent* entry;
    int files_count = 0;
    while ((entry = readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
            files_count++;
        }
    }
    closedir(dir);

    if (files_count == 0) {
        log_info(logger_storage, "Directorio de File %s quedó vacío. Eliminando...", op->nombre_file);
        rmdir(path_file);
    }

    // 6. Loguear éxito y liberar
    log_info(logger_storage, "##%d Tag Eliminado %s:%s", 
             op->query_id, op->nombre_file, op->nombre_tag);
    
    free(path_file); 
    free(path_tag); 
    free(path_metadata); 
    free(path_logical_blocks_dir);
    
    return OP_OK;
}




// --- Implementaciones MOCK (vacías) para las otras operaciones ---
t_codigo_operacion storage_op_commit(t_op_storage* op) {
    log_info(logger_storage, "##%d Commit de File: Tag %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
    return OP_OK;
}






// --- Función auxiliar para reconstruir la cadena de matriz BLOCKS ---
char* build_blocks_string(char** bloques_actuales, int count_actual, int count_nuevo) {
    char* nuevo_array_str = string_new();
    string_append(&nuevo_array_str, "[");

    for (int i = 0; i < count_nuevo; i++) {
        if (i < count_actual) {
            // Copia el bloque existente
            string_append(&nuevo_array_str, bloques_actuales[i]);
        } else {
            // Agrega el nuevo bloque (apuntando a 0) 
            string_append(&nuevo_array_str, "0");
        }
        
        if (i < count_nuevo - 1) {
            string_append(&nuevo_array_str, ",");
        }
    }
    string_append(&nuevo_array_str, "]");
    return nuevo_array_str;
}