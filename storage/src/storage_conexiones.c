#include "storage_conexiones.h"
#include <pthread.h>

// --- Variables Globales para contar Workers ---
static int cantidad_workers_conectados = 0;
pthread_mutex_t mutex_conteo_workers = PTHREAD_MUTEX_INITIALIZER;

// Renombramos "atender_worker" a "gestionar_conexion_worker"
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
    liberar_paquete(paquete_handshake);

    //Cant workers
    pthread_mutex_lock(&mutex_conteo_workers);
    cantidad_workers_conectados++;
    int total_actual = cantidad_workers_conectados; // Copia local
    pthread_mutex_unlock(&mutex_conteo_workers);

    // Logueamos con el valor real
    log_info(logger_storage, "## Se conecta el Worker %d Cantidad de Workers: %d", worker_id, total_actual);

    t_buffer* buffer_rta_handshake = buffer_create(sizeof(uint32_t));
    buffer_add_uint32(buffer_rta_handshake, superblock_configs.blocksize);
    t_paquete* paquete_rta_handshake = empaquetar_buffer(HANDSAHKE_STORAGE_RTA, buffer_rta_handshake);
    enviar_paquete(socket_worker, paquete_rta_handshake);
    log_info(logger_storage, "Handshake con Worker %d completado. Enviando BLOCK_SIZE: %d.", worker_id, superblock_configs.blocksize);


    // --- 2. Loop de Operaciones ---
    while(1) {
        t_paquete* paquete = recibir_paquete(socket_worker);

        if (paquete == NULL) {
            pthread_mutex_lock(&mutex_conteo_workers);
            cantidad_workers_conectados--;
            total_actual = cantidad_workers_conectados;
            pthread_mutex_unlock(&mutex_conteo_workers);

            // Logueamos la desconexion
            log_info(logger_storage, "## Se desconecta el Worker %d Cantidad de Workers: %d", worker_id, total_actual);
        }

        t_codigo_operacion op_respuesta = OP_OK; 
        
        t_op_storage* op_storage = deserializar_op_storage(paquete->buffer, paquete->codigo_operacion);
        
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
            
            case WRITE:
                op_respuesta = storage_op_write(op_storage);
                break; // Se enviará OP_OK u OP_ERROR

            case READ: {
                char* contenido_leido = NULL;
                op_respuesta = storage_op_read(op_storage, &contenido_leido);

                if (op_respuesta == OP_OK) {
                    // --- Enviar respuesta de LECTURA ---
                    // Creamos una op de storage temporal para la respuesta
                    t_op_storage op_rta;
                    op_rta.contenido = contenido_leido; 
                    
                    // Como leemos bloques completos, el tamaño es el BLOCK_SIZE
                    op_rta.tamano_contenido = superblock_configs.blocksize;

                    t_buffer* buffer_rta = serializar_op_storage(&op_rta, READ_RTA);
                    t_paquete* paq_rta = empaquetar_buffer(READ_RTA, buffer_rta);
                    enviar_paquete(socket_worker, paq_rta);
                    
                    free(contenido_leido);
                    
                    // Liberamos y saltamos la respuesta OK/ERROR default
                    destruir_op_storage(op_storage);
                    liberar_paquete(paquete);
                    continue; // Ya enviamos nuestra respuesta
                    
                } else {
                    // Hubo un error en storage_op_read,
                    // 'break' para que se envíe el OP_ERROR
                    free(contenido_leido); // (será NULL, pero por las dudas)
                    break; 
                }
            }

            default:
                log_warning(logger_storage, "Operación desconocida recibida (op_code: %d).", paquete->codigo_operacion);
                op_respuesta = OP_ERROR; 
                break;
        }
        
        destruir_op_storage(op_storage);
        liberar_paquete(paquete); 

        // Enviamos la respuesta (OK o ERROR)
        t_paquete* paquete_respuesta = empaquetar_buffer(op_respuesta, NULL);
        enviar_paquete(socket_worker, paquete_respuesta);
    }

    close(socket_worker);
    return NULL;
}