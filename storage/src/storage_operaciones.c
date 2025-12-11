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
    return S_ISDIR(st.st_mode); 
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
        free(path_file); free(path_tag); free(path_metadata); free(path_logical_blocks);
        return FILE_TAG_PREEXISTENTE; 
    }

    // 3. Crear las estructuras de directorios
    mkdir(path_file, 0777); // Crea el dir del File (si no existe)
    mkdir(path_tag, 0777);  // Crea el dir del Tag
    mkdir(path_logical_blocks, 0777); // Crea el dir logical_blocks

    // 4. Crear y escribir el metadata.config inicial
    FILE* f_metadata = fopen(path_metadata, "w");
    if (f_metadata == NULL) {
        log_error(logger_storage, "##%d Error creando metadata %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
        free(path_file); free(path_tag); free(path_metadata); free(path_logical_blocks);
        return OP_ERROR;
    }

    fprintf(f_metadata, "TAMAÑO=0\n");
    fprintf(f_metadata, "ESTADO=WORK_IN_PROGRESS\n");
    fprintf(f_metadata, "BLOCKS=[]\n");
    fclose(f_metadata);

    // 5. Loguear éxito (Log obligatorio)
    log_info(logger_storage, "##%d File Creado %s:%s", op->query_id, op->nombre_file, op->nombre_tag);

    free(path_file); free(path_tag); free(path_metadata); free(path_logical_blocks);
    return OP_OK;
}

t_codigo_operacion storage_op_truncate(t_op_storage* op) {
    
    // 0. Validar que el tamaño sea múltiplo de BLOCK_SIZE
    if (op->tamano % superblock_configs.blocksize != 0) {
        log_error(logger_storage, "##%d Error: Tamaño TRUNCATE (%d) no es múltiplo de BLOCK_SIZE", op->query_id, op->tamano);
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
        log_error(logger_storage, "##%d Error: Metadata no encontrada %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
        return FILE_TAG_INEXISTENTE; 
    }

    // 3. Chequear estado "COMMITED" 
    char* estado = config_get_string_value(metadata, "ESTADO");
    if (strcmp(estado, "COMMITED") == 0) {
        log_error(logger_storage, "##%d Error: TRUNCATE no permitido en COMMITED", op->query_id);
        config_destroy(metadata);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
        return ESCRITURA_NO_PERMITIDA; 
    }

    // 4. Calcular bloques
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

            // --- Lógica de liberación de bloque físico ---
            struct stat st;
            if (stat(path_bloque_fisico, &st) == 0) {
                // Si nlink == 1 y NO es el bloque 0, liberar
                if (st.st_nlink == 1 && atoi(nro_bloque_fisico_str) != 0) {
                    int bloque_a_liberar = atoi(nro_bloque_fisico_str);
                    liberar_bloque(bloque_a_liberar);       
                    log_info(logger_storage, "##%d Bloque Físico Liberado %d", op->query_id, bloque_a_liberar);
                }
            }
            free(nombre_logico); free(path_bloque_logico); free(path_bloque_fisico);
        }
    }

    // 6. Actualizar y guardar metadata 
    char* nuevo_array_str = build_blocks_string(bloques_actuales_array, bloques_actuales_count, bloques_nuevos_count);
    char* tamano_str = string_itoa(op->tamano);

    config_set_value(metadata, "TAMAÑO", tamano_str);
    config_set_value(metadata, "BLOCKS", nuevo_array_str);
    config_save(metadata);
    
    log_info(logger_storage, "##%d File Truncado %s:%s Tamaño: %d", op->query_id, op->nombre_file, op->nombre_tag, op->tamano);
    
    free(tamano_str); free(nuevo_array_str); free(path_bloque_fisico_0);
    string_array_destroy(bloques_actuales_array);
    config_destroy(metadata);
    free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
    
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
    char* tamano_str = config_get_string_value(metadata_origen, "TAMAÑO");
    char* bloques_str = config_get_string_value(metadata_origen, "BLOCKS");
    char** bloques_array = config_get_array_value(metadata_origen, "BLOCKS");
    int num_bloques = string_array_size(bloques_array);

    FILE* f_metadata_destino = fopen(path_metadata_destino, "w");
    if (f_metadata_destino == NULL) {
        log_error(logger_storage, "##%d Error creando metadata destino", op->query_id);
        config_destroy(metadata_origen); string_array_destroy(bloques_array);
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
        
        free(nombre_logico_destino); free(path_bloque_logico_destino); free(path_bloque_fisico);
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
        return FILE_TAG_INEXISTENTE; 
    }

    // 3. Eliminar Links y Chequear Físicos
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

        struct stat st;
        if (stat(path_bloque_fisico, &st) == 0) {
            // Liberar si nlink == 1 y no es bloque 0
            if (st.st_nlink == 1 && atoi(nro_bloque_fisico_str) != 0) {
                log_info(logger_storage, "Bloque físico %s ya no está referenciado. Liberando...", nro_bloque_fisico_str);
                liberar_bloque(atoi(nro_bloque_fisico_str));            
                log_info(logger_storage, "##%d Bloque Físico Liberado %s", op->query_id, nro_bloque_fisico_str);
            }
        }
        free(nombre_logico); free(path_bloque_logico); free(path_bloque_fisico);
    }

    // 4. Eliminar los archivos y directorios
    string_array_destroy(bloques_array);
    config_destroy(metadata);

    unlink(path_metadata); // Borra metadata.config
    rmdir(path_logical_blocks_dir); // Borra /logical_blocks (debe estar vacío)
    rmdir(path_tag); // Borra /TAG (debe estar vacío)

    // Opcional: Borrar dir del File si está vacío
    DIR* dir = opendir(path_file);
    if (dir) {
        struct dirent* entry;
        int files_count = 0;
        while ((entry = readdir(dir)) != NULL) {
            if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) files_count++;
        }
        closedir(dir);
        if (files_count == 0) {
            rmdir(path_file);
            log_info(logger_storage, "Directorio de File %s eliminado (estaba vacío)", op->nombre_file);
        }
    }
    // 6. Loguear éxito y liberar
    log_info(logger_storage, "##%d Tag Eliminado %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
    free(path_file); free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
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
        log_error(logger_storage, "##%d Error COMMIT: Metadata no encontrada", op->query_id);
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
        char* nro_bloque_fisico_actual_str = bloques_array[i];
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
        
        free(nombre_bloque_fisico_actual); free(path_bloque_fisico_actual); free(hash_actual);
    }
    
    // 5. Guardar cambios en hash_index_config
    config_save(hash_index_config);
    config_destroy(hash_index_config);

    // 6. Actualizar y guardar metadata
    config_set_value(metadata, "ESTADO", "COMMITED"); 
    if(metadata_modificado) {
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
    // Bloque lógico inicial donde empezamos a escribir
    int nro_bloque_logico_inicial = op->direccion_base; 
    
    // 1. Armado de paths básicos
    char* path_tag = string_from_format("%s/files/%s/%s", storage_configs.puntomontaje, op->nombre_file, op->nombre_tag);
    char* path_metadata = string_from_format("%s/metadata.config", path_tag);
    char* path_logical_blocks_dir = string_from_format("%s/logical_blocks", path_tag);

    // 2. Abrir metadata y validaciones iniciales
    t_config* metadata = config_create(path_metadata);
    if (metadata == NULL) {
        log_error(logger_storage, "##%d Error WRITE: Metadata no existe %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
        return FILE_TAG_INEXISTENTE; 
    }

    char* estado = config_get_string_value(metadata, "ESTADO");
    if (strcmp(estado, "COMMITED") == 0) {
        log_error(logger_storage, "##%d WRITE Error: No se puede escribir en un File:Tag COMMITED", op->query_id);
        config_destroy(metadata);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
        return ESCRITURA_NO_PERMITIDA; 
    }
    
    char** bloques_array = config_get_array_value(metadata, "BLOCKS");
    int num_bloques_total = string_array_size(bloques_array);

    // 3. Variables para el bucle de escritura
    int bytes_escritos = 0;
    int bytes_totales = op->tamano_contenido;
    int bloque_logico_actual = nro_bloque_logico_inicial;

    // Validamos que tengamos espacio suficiente asignado (TRUNCATE previo)
    // Calculamos cuántos bloques necesitamos tocar
    int bloques_necesarios = (bytes_totales + superblock_configs.blocksize - 1) / superblock_configs.blocksize;
    
    if (bloque_logico_actual + bloques_necesarios > num_bloques_total) {
        log_error(logger_storage, "##%d Error WRITE: Se intenta escribir fuera del tamaño del archivo. (Inicio: %d, Req: %d, Disp: %d)", 
                  op->query_id, bloque_logico_actual, bloques_necesarios, num_bloques_total);
        config_destroy(metadata); string_array_destroy(bloques_array);
        free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
        return LECTURA_O_ESCRITURA_FUERA_DE_LIMITE;
    }

    // ---------------------------------------------------------
    // 4. BUCLE DE ESCRITURA MULTI-BLOQUE
    // ---------------------------------------------------------
    while (bytes_escritos < bytes_totales) {
        
        // Calculamos cuánto escribir en ESTE bloque (máximo BLOCK_SIZE)
        int bytes_restantes = bytes_totales - bytes_escritos;
        int bytes_a_escribir_ahora = (bytes_restantes < superblock_configs.blocksize) ? bytes_restantes : superblock_configs.blocksize;

        // Puntero al pedazo de contenido actual
        void* contenido_actual = op->contenido + bytes_escritos;

        // Path del bloque lógico actual
        char* path_bloque_logico = string_from_format("%s/%06d.dat", path_logical_blocks_dir, bloque_logico_actual);

        // Obtenemos el físico actual del array en memoria
        char* nro_bloque_fisico_actual_str = bloques_array[bloque_logico_actual];
        int nro_bloque_fisico_actual = atoi(nro_bloque_fisico_actual_str);

        char* path_bloque_fisico_actual = string_from_format("%s/physical_blocks/block%04d.dat", 
                                                            storage_configs.puntomontaje, 
                                                            nro_bloque_fisico_actual);
        
        // Verificamos nlink para Copy-On-Write
        struct stat st;
        stat(path_bloque_fisico_actual, &st);

        // CASO A: COPY-ON-WRITE (Bloque compartido o Bloque 0)
        if (st.st_nlink > 2 || nro_bloque_fisico_actual == 0) {
            log_info(logger_storage, "##%d WRITE (CoW): Bloque Lógico %d apunta a Físico %d (compartido). Separando...", 
                     op->query_id, bloque_logico_actual, nro_bloque_fisico_actual);

            int nuevo_nro_bloque_fisico = reservar_bloque_real(op->query_id);
            
            if (nuevo_nro_bloque_fisico == -1) { 
                // Fallo crítico: Limpieza y retorno
                free(path_bloque_logico); free(path_bloque_fisico_actual);
                string_array_destroy(bloques_array); config_destroy(metadata);
                free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
                return ESPACIO_INSUFICIENTE; 
            }
            
            char* path_nuevo_bloque_fisico = string_from_format("%s/physical_blocks/block%04d.dat", 
                                                                storage_configs.puntomontaje, 
                                                                nuevo_nro_bloque_fisico);
            
            // Escribimos en el NUEVO bloque
            escribir_en_bloque_fisico(path_nuevo_bloque_fisico, contenido_actual, bytes_a_escribir_ahora, superblock_configs.blocksize);

            // Actualizamos Hard Link
            unlink(path_bloque_logico); 
            link(path_nuevo_bloque_fisico, path_bloque_logico); 
            
            // Actualizamos el Array en memoria (Importante para la metadata final)
            free(bloques_array[bloque_logico_actual]);
            bloques_array[bloque_logico_actual] = string_itoa(nuevo_nro_bloque_fisico);

            // Liberamos referencia al viejo si corresponde
            chequear_y_liberar_bloque_fisico(op->query_id, nro_bloque_fisico_actual_str);
            
            free(path_nuevo_bloque_fisico);
            
        } 
        // CASO B: ESCRITURA DIRECTA (bloque no compartido)
        else {
            log_info(logger_storage, "##%d WRITE: Escribiendo directo en Bloque Lógico %d (Físico %d)", 
                     op->query_id, bloque_logico_actual, nro_bloque_fisico_actual);
            
            escribir_en_bloque_fisico(path_bloque_fisico_actual, contenido_actual, bytes_a_escribir_ahora, superblock_configs.blocksize);
        }

        log_info(logger_storage, "##%d Bloque Lógico Escrito %s:%s Número de Bloque: %d", 
             op->query_id, op->nombre_file, op->nombre_tag, bloque_logico_actual);

        // Limpieza de iteración
        free(path_bloque_logico);
        free(path_bloque_fisico_actual);

        // Avanzamos contadores
        bytes_escritos += bytes_a_escribir_ahora;
        bloque_logico_actual++;
    }
    // ---------------------------------------------------------

    // 5. Guardar Metadata Actualizada (Una sola vez al final)
    char* nuevo_blocks_config_str = array_to_blocks_string(bloques_array, num_bloques_total);
    config_set_value(metadata, "BLOCKS", nuevo_blocks_config_str);
    config_save(metadata);
    
    // 6. Liberación Final
    free(nuevo_blocks_config_str);
    string_array_destroy(bloques_array);
    config_destroy(metadata);
    free(path_tag); free(path_metadata); free(path_logical_blocks_dir);
    
    return OP_OK;
}

t_codigo_operacion storage_op_read(t_op_storage* op, char** contenido_leido) {
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
    (*contenido_leido)[superblock_configs.blocksize] = '\0'; 

    // 6. Liberar y loguear
    munmap(buffer_bloque, superblock_configs.blocksize);
    close(fd);
    
    log_info(logger_storage, "##%d Bloque Lógico Leído %s:%s Número de Bloque: %d", 
             op->query_id, op->nombre_file, op->nombre_tag, nro_bloque_logico);
    
    config_destroy(metadata); string_array_destroy(bloques_array);
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
        if (i < count_nuevo - 1) string_append(&nuevo_array_str, ",");
    }
    string_append(&nuevo_array_str, "]");
    return nuevo_array_str;
}

/**
 * @brief Chequea el nlink de un bloque físico y lo marca como libre si ya no se usa.
 */
void chequear_y_liberar_bloque_fisico(int query_id, char* nro_bloque_fisico_str) {
    int nro_bloque_fisico = atoi(nro_bloque_fisico_str);
    if (nro_bloque_fisico == 0) return; 

    char* path_bloque_fisico = string_from_format("%s/physical_blocks/block%04d.dat", 
                                                 storage_configs.puntomontaje, nro_bloque_fisico);
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
        if (i < count - 1) string_append(&nuevo_array_str, ",");
    }
    string_append(&nuevo_array_str, "]");
    return nuevo_array_str;
}

void escribir_en_bloque_fisico(char* path_bloque_fisico, void* contenido, int tamano_contenido, int block_size) {
    usleep(storage_configs.retardoaccesobloque * 1000);

    int fd = open(path_bloque_fisico, O_RDWR);
    if (fd == -1) {
        log_error(logger_storage, "WRITE_HELPER: No se pudo abrir %s", path_bloque_fisico);
        return;
    }
    
    void* buffer_bloque = mmap(NULL, block_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (buffer_bloque == MAP_FAILED) { close(fd); return; }

    // Usamos el tamaño que nos pasan, validando que no se pase del block_size
    int bytes_a_copiar = (tamano_contenido < block_size) ? tamano_contenido : block_size;
    
    memset(buffer_bloque, 0, block_size); // Limpiamos basura anterior
    memcpy(buffer_bloque, contenido, bytes_a_copiar); // Copiamos bytes crudos

    msync(buffer_bloque, block_size, MS_SYNC);
    munmap(buffer_bloque, block_size);
    close(fd);
}

int reservar_bloque_real(int query_id) {
    // Esta función busca Y marca como ocupado en una operación atómica
    int bloque_libre = reservar_bloque_libre(); 

    if (bloque_libre == -1) {
        log_error(logger_storage, "##%d ERROR: File System LLENO. No hay bloques libres.", query_id);
        return -1;
    }

    // Ya no hace falta llamar a 'marcar_bloque_ocupado' porque reservar_bloque_libre ya lo hizo.

    // 3. Asegurar archivo físico
    char* path_bloque = string_from_format("%s/physical_blocks/block%04d.dat", 
                                          storage_configs.puntomontaje, 
                                          bloque_libre);
    
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