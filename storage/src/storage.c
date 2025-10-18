#include "storage.h"
#include "fresh_start.h"

void* atender_worker(void* arg);

int main(int argc, char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Uso: %s [archivo_config]\n", argv[0]);
        return EXIT_FAILURE;
    }

    // 1. Cargar configuración y logger
    inicializar_configs(argv[1]);
    inicializar_logger_storage(storage_configs.loglevel);
    inicializar_superblock_configs(); 
    log_info(logger_storage, "## Storage inicializado.");

    // 2. Inicializar el File System si es FRESH_START
    inicializar_fs(); 

    // 3. Iniciar el servidor
    char* puerto_str = string_itoa(storage_configs.puertoescucha);
    int socket_servidor = iniciar_servidor(puerto_str);
    log_info(logger_storage, "## Storage escuchando en puerto %s", puerto_str);
    free(puerto_str);

    // 4. Aceptar Workers indefinidamente
    while(1) {
        int socket_cliente = esperar_cliente(socket_servidor);
        if (socket_cliente > 0) {
            pthread_t hilo_worker;
            int* cliente_socket_ptr = malloc(sizeof(int));
            *cliente_socket_ptr = socket_cliente;
            pthread_create(&hilo_worker, NULL, (void*) atender_worker, (void*) cliente_socket_ptr);
            pthread_detach(hilo_worker); 
        }
    }

    destruir_logger();
    destruir_configs();
    return EXIT_SUCCESS;
}

void* atender_worker(void* arg) {
    int socket_worker = *((int*) arg);
    free(arg);

    // --- 1. Handshake Inicial ---
    // El worker se conecta y envía su ID 
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
    
    // Respondemos al Worker con el BLOCK_SIZE 
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

        t_codigo_operacion op_respuesta = OP_OK; // Respuesta por defecto 
        t_operacion_query* op_query = NULL;      // Para deserializar los datos

        switch (paquete->codigo_operacion) {
            
            case CREATE:
                op_query = deserializar_operacion_query(paquete->buffer);
                // Se debe captura ID
                log_info(logger_storage, "##<0> File Creado %s:%s", op_query->file, op_query->tag);
                
                destruir_operacion_query(op_query); // Liberamos la memoria
                break;
                
            case TRUNCATE:
                op_query = deserializar_operacion_query(paquete->buffer);
                log_info(logger_storage, "##<0> File Truncado %s:%s Tamaño: %s", op_query->file, op_query->tag, op_query->informacion);
                destruir_operacion_query(op_query);
                break;

            case DELETE:
                op_query = deserializar_operacion_query(paquete->buffer);
                log_info(logger_storage, "##<0> Tag Eliminado %s:%s", op_query->file, op_query->tag);
                destruir_operacion_query(op_query);
                break;

            case COMMIT:
                op_query = deserializar_operacion_query(paquete->buffer);
                log_info(logger_storage, "##<0> Commit de File: Tag %s:%s", op_query->file, op_query->tag);
                destruir_operacion_query(op_query);
                break;

            case TAG:
                op_query = deserializar_operacion_query(paquete->buffer);
                // (Ajustar t_operacion_query y serializacion para esto
                log_info(logger_storage, "##<0> Tag creado %s:%s", op_query->file, op_query->tag);
                destruir_operacion_query(op_query);
                break;
            
            // --- Falta READ y WRITE---

            default:
                log_warning(logger_storage, "Operación desconocida recibida (op_code: %d).", paquete->codigo_operacion);
                op_respuesta = OP_ERROR; // Respondemos con error
                break;
        }
        
        liberar_paquete(paquete); // Liberamos el paquete que recibimos

        // Enviamos la respuesta "OK" 
        t_buffer* buffer_respuesta = buffer_create(0); // Buffer vacío
        t_paquete* paquete_respuesta = empaquetar_buffer(op_respuesta, buffer_respuesta);
        enviar_paquete(socket_worker, paquete_respuesta);
    }

    close(socket_worker);
    return NULL;
}