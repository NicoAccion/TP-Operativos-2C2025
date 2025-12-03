#include "storage_operaciones.h"
#include "bitmap.h"

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
        return FILE_TAG_PREEXISTENTE; // Devolvemos error
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
        return LECTURA_O_ESCRITURA_FUERA_DE_LIMITE;
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
        return FILE_TAG_INEXISTENTE; // Error: File / Tag inexistente
    }

    // 3. Chequear estado "COMMITED" 
    char* estado = config_get_string_value(metadata, "ESTADO");
    if (strcmp(estado, "COMMITED") == 0) {
        log_error(logger_storage, "##%d Error: No se puede truncar un File:Tag en estado COMMITED", op->query_id);
        config_destroy(metadata);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
        return ESCRITURA_NO_PERMITIDA; // Error: Escritura no permitida
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
                    int bloque_a_liberar = atoi(nro_bloque_fisico_str);
                    liberar_bloque(bloque_a_liberar);       
                    log_info(logger_storage, "##%d Bloque Físico Liberado %d", op->query_id, bloque_a_liberar);

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
        return FILE_TAG_INEXISTENTE;
    }

    // 3. Validar Destino (File / Tag preexistente)
    if (directorio_existe(path_tag_destino)) {
        log_error(logger_storage, "##%d Error: File/Tag destino %s:%s ya existe", op->query_id, op->nombre_file_destino, op->nombre_tag_destino);
        config_destroy(metadata_origen);
        free(path_tag_origen); free(path_metadata_origen); free(path_file_destino);
        free(path_tag_destino); free(path_metadata_destino); free(path_logical_blocks_destino);
        return FILE_TAG_PREEXISTENTE;
    }

    // 4. Crear directorios de Destino
    mkdir(path_file_destino, 0777);
    mkdir(path_tag_destino, 0777);
    mkdir(path_logical_blocks_destino, 0777);

    // 5. Leer datos del metadata origen
    char* tamano_str = config_get_string_value(metadata_origen, "TAMANIO");
    char* bloques_str = config_get_string_value(metadata_origen, "BLOQUES");
    char** bloques_array = config_get_array_value(metadata_origen, "BLOQUES");
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
        return ESPACIO_INSUFICIENTE;
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
        return FILE_TAG_INEXISTENTE; // Error: File / Tag inexistente
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
                liberar_bloque(atoi(nro_bloque_fisico_str));            
                
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

t_codigo_operacion storage_op_commit(t_op_storage* op) {
    log_info(logger_storage, "Iniciando COMMIT para %s:%s", op->nombre_file, op->nombre_tag);

    // 1. Armar Paths
    char* path_tag = string_from_format("%s/files/%s/%s", 
                                      storage_configs.puntomontaje, op->nombre_file, op->nombre_tag);
    char* path_metadata = string_from_format("%s/metadata.config", path_tag);
    char* path_logical_blocks_dir = string_from_format("%s/logical_blocks", path_tag);
    char* path_hash_index = string_from_format("%s/blocks_hash_index.config", storage_configs.puntomontaje);

    // 2. Abrir metadata
    t_config* metadata = config_create(path_metadata);
    if (metadata == NULL) {
        log_error(logger_storage, "##%d Error: No se encontró metadata para %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir); free(path_hash_index);
        return FILE_TAG_INEXISTENTE; 
    }

    char* estado = config_get_string_value(metadata, "ESTADO");
    if (strcmp(estado, "COMMITED") == 0) {
        log_info(logger_storage, "##%d File:Tag %s:%s ya estaba en estado COMMITED. No se hace nada.", op->query_id, op->nombre_file, op->nombre_tag);
        config_destroy(metadata);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir); free(path_hash_index);
        return OP_OK; 
    }

    // 3. Abrir hash index
    t_config* hash_index_config = config_create(path_hash_index);
    if (hash_index_config == NULL) {
         log_error(logger_storage, "##%d Error: No se pudo abrir blocks_hash_index.config", op->query_id);
         config_destroy(metadata);
         free(path_tag); free(path_metadata); free(path_logical_blocks_dir); free(path_hash_index);
         return FILE_TAG_INEXISTENTE;
    }

    // 4. Iterar bloques lógicos
    char** bloques_array = config_get_array_value(metadata, "BLOCKS");
    int num_bloques = string_array_size(bloques_array);
    bool metadata_modificado = false; // Flag para saber si debemos guardar

    for (int i = 0; i < num_bloques; i++) {
        char* nro_bloque_fisico_actual_str = bloques_array[i]; // 
        char* nombre_bloque_fisico_actual = string_from_format("block%04d", atoi(nro_bloque_fisico_actual_str));
        char* path_bloque_fisico_actual = string_from_format("%s/physical_blocks/%s.dat", 
                                                            storage_configs.puntomontaje,
                                                            nombre_bloque_fisico_actual);
        
        // --- 4.a. Calcular MD5 (Bloque corregido) ---
        char* hash_actual;
        int fd_bloque = open(path_bloque_fisico_actual, O_RDONLY);
        if (fd_bloque == -1) {
            log_error(logger_storage, "##%d COMMIT: No se pudo abrir el bloque físico %s", op->query_id, nombre_bloque_fisico_actual);
            hash_actual = string_duplicate(""); // Hash vacío para evitar fallos
        } else {
            // Mapeamos el archivo (bloque) a memoria
            void* buffer_bloque = mmap(NULL, superblock_configs.blocksize, PROT_READ, MAP_SHARED, fd_bloque, 0);
            if (buffer_bloque == MAP_FAILED) {
                log_error(logger_storage, "##%d COMMIT: No se pudo mapear el bloque %s", op->query_id, nombre_bloque_fisico_actual);
                hash_actual = string_duplicate("");
            } else {
                hash_actual = crypto_md5(buffer_bloque, superblock_configs.blocksize);
                munmap(buffer_bloque, superblock_configs.blocksize); // Liberamos el mapeo
            }
            close(fd_bloque); // Cerramos el archivo
        }
        // --- Fin bloque MD5 ---

        // 4.b. Buscar hash en index 
        char* nombre_bloque_existente = config_get_string_value(hash_index_config, hash_actual);

        if (nombre_bloque_existente != NULL && strcmp(nombre_bloque_existente, nombre_bloque_fisico_actual) != 0) {
            // --- Deduplicación --- 
            log_info(logger_storage, "##%d Deduplicación: Bloque Lógico %d (hash: %s) puede usar bloque físico %s",
                     op->query_id, i, hash_actual, nombre_bloque_existente);

            char* nro_bloque_fisico_existente_str = string_substring(nombre_bloque_existente, 5, 4); 
            char* path_bloque_fisico_existente = string_from_format("%s/physical_blocks/%s.dat", 
                                                                   storage_configs.puntomontaje,
                                                                   nombre_bloque_existente);
            char* path_bloque_logico = string_from_format("%s/%06d.dat", path_logical_blocks_dir, i);

            unlink(path_bloque_logico); // 1. Eliminar link actual
            link(path_bloque_fisico_existente, path_bloque_logico); // 2. Crear nuevo link
            
            chequear_y_liberar_bloque_fisico(op->query_id, nro_bloque_fisico_actual_str);
            
            // 4. Actualizar array en memoria para guardar en metadata
            free(bloques_array[i]);
            bloques_array[i] = string_itoa(atoi(nro_bloque_fisico_existente_str)); 
            metadata_modificado = true; // Marcamos que el metadata cambió

            log_info(logger_storage, "##%d Deduplicación de Bloque: %s:%s Bloque Lógico %d se reasigna de %s a %s",
                     op->query_id, op->nombre_file, op->nombre_tag, i, nro_bloque_fisico_actual_str, nro_bloque_fisico_existente_str);

            free(nro_bloque_fisico_existente_str);
            free(path_bloque_fisico_existente);
            free(path_bloque_logico);

        } else if (nombre_bloque_existente == NULL) {
            // --- Hash no existe, agregarlo --- 
            log_info(logger_storage, "Hash %s no encontrado. Agregando al índice (Bloque: %s)", hash_actual, nombre_bloque_fisico_actual);
            config_set_value(hash_index_config, hash_actual, nombre_bloque_fisico_actual);
        }
        
        free(nombre_bloque_fisico_actual);
        free(path_bloque_fisico_actual);
        free(hash_actual);
    }
    
    // 5. Guardar cambios en hash_index_config
    config_save(hash_index_config);
    config_destroy(hash_index_config);

    // 6. Actualizar y guardar metadata
    config_set_value(metadata, "ESTADO", "COMMITED"); 
    
    if(metadata_modificado) {
        // Solo re-escribimos el array BLOCKS si hubo deduplicación
        char* nuevo_blocks_config_str = array_to_blocks_string(bloques_array, num_bloques);
        
        config_set_value(metadata, "BLOCKS", nuevo_blocks_config_str);
        
        free(nuevo_blocks_config_str);
    }
    
    config_save(metadata);
    
    log_info(logger_storage, "##%d Commit de File: Tag %s:%s", 
             op->query_id, op->nombre_file, op->nombre_tag); 
    
    // 7. Liberar todo
    string_array_destroy(bloques_array);
    config_destroy(metadata);
    free(path_tag); 
    free(path_metadata); 
    free(path_logical_blocks_dir);
    free(path_hash_index);

    return OP_OK;
}

t_codigo_operacion storage_op_write(t_op_storage* op) {
    
    //int nro_bloque_logico = op->direccion_base / superblock_configs.blocksize;
    int nro_bloque_logico = op->direccion_base; 
    // 1. Armar paths
    char* path_tag = string_from_format("%s/files/%s/%s", 
                                      storage_configs.puntomontaje, op->nombre_file, op->nombre_tag);
    char* path_metadata = string_from_format("%s/metadata.config", path_tag);
    char* path_logical_blocks_dir = string_from_format("%s/logical_blocks", path_tag);
    char* path_bloque_logico = string_from_format("%s/%06d.dat", path_logical_blocks_dir, nro_bloque_logico);

    // 2. Abrir metadata y validar
    t_config* metadata = config_create(path_metadata);
    if (metadata == NULL) {
        log_error(logger_storage, "##%d WRITE Error: No se encontró metadata para %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir); free(path_bloque_logico);
        return FILE_TAG_INEXISTENTE; // Error: File / Tag inexistente
    }

    // 3. Chequear estado "COMMITED"
    char* estado = config_get_string_value(metadata, "ESTADO");
    if (strcmp(estado, "COMMITED") == 0) {
        log_error(logger_storage, "##%d WRITE Error: No se puede escribir en un File:Tag COMMITED", op->query_id);
        config_destroy(metadata);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir); free(path_bloque_logico);
        return ESCRITURA_NO_PERMITIDA; // Error: Escritura no permitida
    }
    
    // 4. Chequear fuera de límite
    char** bloques_array = config_get_array_value(metadata, "BLOCKS");
    int num_bloques = string_array_size(bloques_array);

    if (nro_bloque_logico >= num_bloques) {
        log_error(logger_storage, "##%d WRITE Error: Bloque lógico %d fuera de límite (Tamaño: %d bloques)", op->query_id, nro_bloque_logico, num_bloques);
        config_destroy(metadata);
        string_array_destroy(bloques_array);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir); free(path_bloque_logico);
        return LECTURA_O_ESCRITURA_FUERA_DE_LIMITE; // Error: Lectura o escritura fuera de limite
    }
    
    // 5. Lógica Copy-on-Write (CoW)
    char* nro_bloque_fisico_actual_str = bloques_array[nro_bloque_logico];
    char* path_bloque_fisico_actual = string_from_format("%s/physical_blocks/block%04d.dat", 
                                                        storage_configs.puntomontaje, 
                                                        atoi(nro_bloque_fisico_actual_str));
    struct stat st;
    stat(path_bloque_fisico_actual, &st);

    // Si nlink > 2 (alguien más lo usa) O si es el bloque 0, necesitamos copiar.
        if (st.st_nlink > 2 || atoi(nro_bloque_fisico_actual_str) == 0) {
        log_info(logger_storage, "##%d WRITE (CoW): Bloque físico %s es compartido. Realizando copia.", op->query_id, nro_bloque_fisico_actual_str);

        // a. Encontrar bloque nuevo (REAL - Usando Bitmap)
        
        int nuevo_nro_bloque_fisico = reservar_bloque_real(op->query_id);
        
        if (nuevo_nro_bloque_fisico == -1) { 
            // Si no hay bloques, limpiamos y devolvemos error
            log_error(logger_storage, "##%d Fallo CoW: No hay espacio para copiar bloque.", op->query_id);
            string_array_destroy(bloques_array);
            config_destroy(metadata);
            free(path_tag); free(path_metadata); free(path_logical_blocks_dir); 
            free(path_bloque_logico); free(path_bloque_fisico_actual);
            return ESPACIO_INSUFICIENTE; 
        }
        
        char* path_nuevo_bloque_fisico = string_from_format("%s/physical_blocks/block%04d.dat", 
                                                            storage_configs.puntomontaje, 
                                                            nuevo_nro_bloque_fisico);
        
        // b. Escribir contenido en el *nuevo* bloque
        escribir_en_bloque_fisico(path_nuevo_bloque_fisico, op->contenido, op->tamano_contenido, superblock_configs.blocksize);

        // c. Actualizar hard link
        unlink(path_bloque_logico); // Borrar link al bloque viejo
        link(path_nuevo_bloque_fisico, path_bloque_logico); // Crear link al bloque nuevo
        
        // d. Actualizar metadata
        free(bloques_array[nro_bloque_logico]);
        bloques_array[nro_bloque_logico] = string_itoa(nuevo_nro_bloque_fisico);
        
        char* nuevo_blocks_config_str = array_to_blocks_string(bloques_array, num_bloques);
        config_set_value(metadata, "BLOCKS", nuevo_blocks_config_str);
        config_save(metadata);
        
        free(nuevo_blocks_config_str);
        free(path_nuevo_bloque_fisico);

        // e. Chequear si el bloque viejo quedó libre
        chequear_y_liberar_bloque_fisico(op->query_id, nro_bloque_fisico_actual_str);
    
    } else {
        // --- Escribir directo ---
        // nlink == 2 (solo /physical_blocks y este /logical_blocks) y no es bloque 0
        log_info(logger_storage, "##%d WRITE: Escribiendo directo en bloque físico %s", op->query_id, nro_bloque_fisico_actual_str);
        escribir_en_bloque_fisico(path_bloque_fisico_actual, op->contenido, op->tamano_contenido, superblock_configs.blocksize);
    }

    log_info(logger_storage, "##%d Bloque Lógico Escrito %s:%s Número de Bloque: %d", 
             op->query_id, op->nombre_file, op->nombre_tag, nro_bloque_logico);

    // 6. Liberar
    string_array_destroy(bloques_array);
    config_destroy(metadata);
    free(path_tag); free(path_metadata); free(path_logical_blocks_dir); 
    free(path_bloque_logico); free(path_bloque_fisico_actual);
    
    return OP_OK;
}

t_codigo_operacion storage_op_read(t_op_storage* op, char** contenido_leido) {
    
    // Asumimos que `direccion_base` es el `nro_bloque_logico`
    int nro_bloque_logico = op->direccion_base; 

    // 1. Armar paths
    char* path_tag = string_from_format("%s/files/%s/%s", 
                                      storage_configs.puntomontaje, op->nombre_file, op->nombre_tag);
    char* path_metadata = string_from_format("%s/metadata.config", path_tag);

    // 2. Abrir metadata y validar
    t_config* metadata = config_create(path_metadata);
    if (metadata == NULL) {
        log_error(logger_storage, "##%d READ Error: No se encontró metadata para %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
        free(path_tag); free(path_metadata);
        return FILE_TAG_INEXISTENTE; // Error: File / Tag inexistente
    }
    
    // 3. Chequear fuera de límite
    char** bloques_array = config_get_array_value(metadata, "BLOCKS");
    int num_bloques = string_array_size(bloques_array);

    if (nro_bloque_logico >= num_bloques) {
        log_error(logger_storage, "##%d READ Error: Bloque lógico %d fuera de límite (Tamaño: %d bloques)", op->query_id, nro_bloque_logico, num_bloques);
        config_destroy(metadata);
        string_array_destroy(bloques_array);
        free(path_tag); free(path_metadata);
        return LECTURA_O_ESCRITURA_FUERA_DE_LIMITE; // Error: Lectura o escritura fuera de limite
    }
    
    // 4. Leer el bloque físico
    char* nro_bloque_fisico_str = bloques_array[nro_bloque_logico];
    char* path_bloque_fisico = string_from_format("%s/physical_blocks/block%04d.dat", 
                                                    storage_configs.puntomontaje, 
                                                    atoi(nro_bloque_fisico_str));

    usleep(storage_configs.retardoaccesobloque * 1000);

    int fd = open(path_bloque_fisico, O_RDONLY);
/*    if (fd == -1) {
        log_error(logger_storage, "##%d READ Error: No se pudo abrir bloque físico %s", op->query_id, nro_bloque_fisico_str);
        config_destroy(metadata); string_array_destroy(bloques_array);
        free(path_tag); free(path_metadata); free(path_bloque_fisico);
        return OP_ERROR;
    }*/
    
    // Mapeamos
    void* buffer_bloque = mmap(NULL, superblock_configs.blocksize, PROT_READ, MAP_SHARED, fd, 0);
    if (buffer_bloque == MAP_FAILED) {
        log_error(logger_storage, "##%d READ Error: mmap falló para bloque %s", op->query_id, nro_bloque_fisico_str);
        close(fd);
        config_destroy(metadata); string_array_destroy(bloques_array);
        free(path_tag); free(path_metadata); free(path_bloque_fisico);
        return OP_ERROR;
    }

    // 5. Copiar contenido al out-parameter
    *contenido_leido = malloc(superblock_configs.blocksize + 1);
    memcpy(*contenido_leido, buffer_bloque, superblock_configs.blocksize);
    (*contenido_leido)[superblock_configs.blocksize] = '\0'; // Aseguramos null-terminator

    // 6. Liberar y loguear
    munmap(buffer_bloque, superblock_configs.blocksize);
    close(fd);
    
    log_info(logger_storage, "##%d Bloque Lógico Leído %s:%s Número de Bloque: %d", 
             op->query_id, op->nombre_file, op->nombre_tag, nro_bloque_logico);
    
    config_destroy(metadata);
    string_array_destroy(bloques_array);
    free(path_tag); free(path_metadata); free(path_bloque_fisico);
    
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

/**
 * @brief Chequea el nlink de un bloque físico y lo marca como libre si ya no se usa.
 */
void chequear_y_liberar_bloque_fisico(int query_id, char* nro_bloque_fisico_str) {
    int nro_bloque_fisico = atoi(nro_bloque_fisico_str);
    if (nro_bloque_fisico == 0) return; // Nunca liberar el bloque 0?

    char* path_bloque_fisico = string_from_format("%s/physical_blocks/block%04d.dat", 
                                                 storage_configs.puntomontaje, 
                                                 nro_bloque_fisico);
    struct stat st;
    if (stat(path_bloque_fisico, &st) == 0) {
        // nlink == 1 significa que solo existe el archivo en /physical_blocks
        if (st.st_nlink == 1) { 
            log_info(logger_storage, "Bloque físico %d ya no está referenciado. Liberando...", nro_bloque_fisico);
        
            liberar_bloque(nro_bloque_fisico);

            log_info(logger_storage, "##%d Bloque Físico Liberado %d", query_id, nro_bloque_fisico);
        }
    }
    else {
        log_warning(logger_storage, "No se pudo obtener stat() del bloque físico %d", nro_bloque_fisico);
    }
    
    free(path_bloque_fisico);
}

// Simplemente une un array de strings con comas, [a,b,c]
char* array_to_blocks_string(char** bloques_array, int count) {
    char* nuevo_array_str = string_new();
    string_append(&nuevo_array_str, "[");

    for (int i = 0; i < count; i++) {
        string_append(&nuevo_array_str, bloques_array[i]);
        if (i < count - 1) {
            string_append(&nuevo_array_str, ",");
        }
    }
    string_append(&nuevo_array_str, "]");
    return nuevo_array_str;
}

/*// --- SIMULACIÓN DE BITMAP ---
// Esto simula encontrar un bloque libre. Empieza en 1 (0 es 'initial_file').
static int mock_next_free_block = 1;

int encontrar_bloque_libre_mock(int query_id) {
    int nuevo_nro_bloque = mock_next_free_block;
    mock_next_free_block++;

    char* path_nuevo_bloque = string_from_format("%s/physical_blocks/block%04d.dat", 
                                                storage_configs.puntomontaje, 
                                                nuevo_nro_bloque);

    // Creamos el archivo físico vacío
    int fd = open(path_nuevo_bloque, O_RDWR | O_CREAT, 0666);
    if (fd == -1) {
        log_error(logger_storage, "##%d Mock-Bitmap: Error al crear archivo para bloque físico %d", query_id, nuevo_nro_bloque);
        free(path_nuevo_bloque);
        return -1; // Error
    }
    
    // Lo "llenamos" con ceros (o cualquier placeholder)
    ftruncate(fd, superblock_configs.blocksize);
    close(fd);

    log_info(logger_storage, "##%d Bloque Físico Reservado %d (Simulación)", query_id, nuevo_nro_bloque);
    free(path_nuevo_bloque);
    return nuevo_nro_bloque;
}*/

// --- Helper para escribir en un bloque físico ---
void escribir_en_bloque_fisico(char* path_bloque_fisico, void* contenido, int tamano_contenido, int block_size) {

    usleep(storage_configs.retardoaccesobloque * 1000);

    int fd = open(path_bloque_fisico, O_RDWR);
    if (fd == -1) {
        log_error(logger_storage, "WRITE_HELPER: No se pudo abrir %s", path_bloque_fisico);
        return;
    }
    
    void* buffer_bloque = mmap(NULL, block_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (buffer_bloque == MAP_FAILED) {
        log_error(logger_storage, "WRITE_HELPER: mmap falló");
        close(fd);
        return;
    }

    // Usamos el tamaño que nos pasan, validando que no se pase del block_size
    int bytes_a_copiar = (tamano_contenido < block_size) ? tamano_contenido : block_size;
    
    memset(buffer_bloque, 0, block_size); // Limpiamos basura anterior
    memcpy(buffer_bloque, contenido, bytes_a_copiar); // Copiamos bytes crudos

    msync(buffer_bloque, block_size, MS_SYNC);
    munmap(buffer_bloque, block_size);
    close(fd);
}

int reservar_bloque_real(int query_id) {
    // 1. Buscar bit libre en el bitarray
    int bloque_libre = buscar_bloque_libre(); 

    if (bloque_libre == -1) {
        log_error(logger_storage, "##%d ERROR: File System LLENO. No hay bloques libres.", query_id);
        return -1;
    }

    // 2. Marcar como ocupado en el bitarray y guardar en disco
    marcar_bloque_ocupado(bloque_libre); 

    // 3. Asegurar que existe el archivo físico (blockXXXX.dat)
    char* path_bloque = string_from_format("%s/physical_blocks/block%04d.dat", 
                                          storage_configs.puntomontaje, 
                                          bloque_libre);
    
    // Lo abrimos con O_CREAT para asegurarnos que exista.
    // Si ya existía (de una ejecución anterior), no pasa nada, se sobrescribe
    int fd = open(path_bloque, O_RDWR | O_CREAT, 0666);
    if (fd == -1) {
        log_error(logger_storage, "##%d ERROR: No se pudo crear archivo físico para bloque %d", query_id, bloque_libre);
        liberar_bloque(bloque_libre); // Rollback
        free(path_bloque);
        return -1;
    }

    // Aseguramos el tamaño
    ftruncate(fd, superblock_configs.blocksize);
    close(fd);

    log_info(logger_storage, "##%d Bloque Físico Reservado %d (Real)", query_id, bloque_libre);
    free(path_bloque);
    return bloque_libre;
}