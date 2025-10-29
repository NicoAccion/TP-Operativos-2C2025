#include "storage_operaciones.h"

/**
 * @brief Función auxiliar para validar si un directorio existe.
 * Usa stat() como pide el enunciado.
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


// --- Implementaciones MOCK (vacías) para las otras operaciones ---

t_codigo_operacion storage_op_truncate(t_op_storage* op) {
    log_info(logger_storage, "##%d File Truncado %s:%s Tamaño: %d", op->query_id, op->nombre_file, op->nombre_tag, op->tamano);
    return OP_OK;
}

t_codigo_operacion storage_op_delete(t_op_storage* op) {
    log_info(logger_storage, "##%d Tag Eliminado %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
    return OP_OK;
}

t_codigo_operacion storage_op_commit(t_op_storage* op) {
    log_info(logger_storage, "##%d Commit de File: Tag %s:%s", op->query_id, op->nombre_file, op->nombre_tag);
    return OP_OK;
}

t_codigo_operacion storage_op_tag(t_op_storage* op) {
    log_info(logger_storage, "##%d Tag creado %s:%s -> %s:%s", op->query_id, op->nombre_file, op->nombre_tag, op->nombre_file_destino, op->nombre_tag_destino);
    return OP_OK;
}