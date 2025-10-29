#include "storage_conexiones.h"

// Renombramos "atender_worker" a "gestionar_conexion_worker"
// El código de adentro es IDÉNTICO al que ya teníamos.
void* gestionar_conexion_worker(void* arg) {
    int socket_worker = *((int*) arg);
    free(arg);

    // --- 1. Handshake Inicial ---
    t_paquete* paquete_handshake = recibir_paquete(socket_worker);
    uint32_t worker_id;
    if (paquete_handshake == NULL || paquete_handshake->codigo_operacion != HANDSHAKE_WORKER) {
        log_error(logger_storage, "## Se desconecta un Worker (falló el handshake inicial).");
        if (paquete_handshake) liberar_paquete(paquete_handshake);
        close(socket_worker);
        return NULL;
    }
    worker_id = deserializar_worker(paquete_handshake->buffer);
    log_info(logger_storage, "## Se conecta el Worker %d Cantidad de Workers: <CANTIDAD_MOCK>", worker_id); 
    liberar_paquete(paquete_handshake);
    
    t_buffer* buffer_rta_handshake = buffer_create(sizeof(uint32_t));
    buffer_add_uint32(buffer_rta_handshake, superblock_configs.blocksize);
    t_paquete* paquete_rta_handshake = empaquetar_buffer(HANDSAHKE_STORAGE_RTA, buffer_rta_handshake);
    enviar_paquete(socket_worker, paquete_rta_handshake);
    log_info(logger_storage, "Handshake con Worker %d completado. Enviando BLOCK_SIZE: %d.", worker_id, superblock_configs.blocksize);


    // --- 2. Loop de Operaciones ---
    while(1) {
        t_paquete* paquete = recibir_paquete(socket_worker);

        if (paquete == NULL) {
            log_info(logger_storage, "## Se desconecta el Worker %d Cantidad de Workers: <CANTIDAD_MOCK>", worker_id); 
            break; 
        }

        t_codigo_operacion op_respuesta = OP_OK; 
        
        t_op_storage* op_storage = deserializar_op_storage(paquete->buffer, paquete->codigo_operacion);
        
        // (Aquí podríamos asignar el worker_id al op_storage si lo necesitáramos para los logs)
        // op_storage->worker_id = worker_id; 

        switch (paquete->codigo_operacion) {
            
            case CREATE:
                op_respuesta = storage_op_create(op_storage);
                break;
                
            case TRUNCATE:
                op_respuesta = storage_op_truncate(op_storage);
                break;

            case DELETE:
                op_respuesta = storage_op_delete(op_storage);
                break;

            case COMMIT:
                op_respuesta = storage_op_commit(op_storage);
                break;

            case TAG:
                op_respuesta = storage_op_tag(op_storage);
                break;
            
            // --- Falta READ y WRITE---

            default:
                log_warning(logger_storage, "Operación desconocida recibida (op_code: %d).", paquete->codigo_operacion);
                op_respuesta = OP_ERROR; 
                break;
        }
        
        destruir_op_storage(op_storage);
        liberar_paquete(paquete); 

        // Enviamos la respuesta (OK o ERROR)
        t_buffer* buffer_respuesta = buffer_create(0); 
        t_paquete* paquete_respuesta = empaquetar_buffer(op_respuesta, buffer_respuesta);
        enviar_paquete(socket_worker, paquete_respuesta);
    }

    close(socket_worker);
    return NULL;
}