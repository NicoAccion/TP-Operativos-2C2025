#include "worker.h"
#include "worker_memoria.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>


int main(int argc, char* argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Uso: %s [archivo_config] [ID_worker]\n", argv[0]);
        return EXIT_FAILURE;
    }
    char* path_config = argv[1];
    uint32_t id_worker = atoi(argv[2]);

    inicializar_configs(path_config);
    inicializar_logger_worker(worker_configs.loglevel);
    log_info(logger_worker, "## Worker %d inicializado", id_worker);

// ===== Conexión con Storage =====
    char* puerto_storage_str = string_itoa(worker_configs.puertostorage);
    int socket_storage = crear_conexion(worker_configs.ipstorage, puerto_storage_str);
    free(puerto_storage_str);
    if (socket_storage < 0) {
        log_error(logger_worker, "Error al conectar con Storage");
        return EXIT_FAILURE;
    }
    log_info(logger_worker, "Conectado a Storage. Realizando handshake...");

    // 1. Enviar ID de Worker a Storage
    t_buffer* buffer_id_storage = serializar_worker(id_worker);
    t_paquete* paquete_handshake_storage = empaquetar_buffer(HANDSHAKE_WORKER, buffer_id_storage);
    enviar_paquete(socket_storage, paquete_handshake_storage);

    // 2. Recibir BLOCK_SIZE de Storage
    t_paquete* rta_handshake_storage = recibir_paquete(socket_storage);
    if (rta_handshake_storage == NULL || rta_handshake_storage->codigo_operacion != HANDSAHKE_STORAGE_RTA) {
        log_error(logger_worker, "Error en handshake con Storage. Storage desconectado.");
        if(rta_handshake_storage) liberar_paquete(rta_handshake_storage);
        close(socket_storage);
        return EXIT_FAILURE;
    }
    
    // 3. Deserializar el BLOCK_SIZE y liberar paquete
    uint32_t block_size = buffer_read_uint32(rta_handshake_storage->buffer);
    liberar_paquete(rta_handshake_storage);

    log_info(logger_worker, "Handshake con Storage OK. BLOCK_SIZE recibido: %d", block_size);
    
    // 4. Inicializar memoria AHORA con el block_size
    inicializar_memoria(worker_configs.tammemoria, block_size);

    // ===== Conexión con Master =====
    char* puerto_master_str = string_itoa(worker_configs.puertomaster);
    int socket_master = crear_conexion(worker_configs.ipmaster, puerto_master_str);
    free(puerto_master_str);
    if (socket_master < 0) {
        log_error(logger_worker, "Error al conectar con Master");
        close(socket_storage);
        return EXIT_FAILURE;
    }
    log_info(logger_worker, "Conectado a Master");

    t_buffer* buffer_id = serializar_worker(id_worker);
    t_paquete* paquete_w = empaquetar_buffer(HANDSHAKE_WORKER, buffer_id);
    enviar_paquete(socket_master, paquete_w);
    
    log_info(logger_worker, "Worker %d en espera de Queries...", id_worker);

    // ===== Bucle principal: esperar y ejecutar Queries =====
    while(1) {
            t_paquete* paquete_master = recibir_paquete(socket_master); 

            if (!paquete_master) {
                log_error(logger_worker, "Se perdió la conexión con Master.");
                break;
            }
            switch (paquete_master->codigo_operacion) {
            
            // --- NUEVA QUERY O RE-EJECUCIÓN ---
            case PAQUETE_QUERY_EJECUCION: { 
                // 1. Deserializamos el paquete correcto
                t_query_ejecucion* query_recibida = deserializar_query_ejecucion(paquete_master->buffer);

                // 2. Logueamos
                log_info(logger_worker, "## Query %d: Se recibe la Query. El path de operaciones es: %s (PC: %d)", 
                         query_recibida->id_query, 
                         query_recibida->archivo_query,
                         query_recibida->program_counter);

                // 3. Llamamos al interpeter
                ejecutar_query(query_recibida->id_query, 
                               query_recibida->archivo_query, 
                               query_recibida->program_counter, // Pasamos el PC
                               socket_master, 
                               socket_storage);
                
                // 4. Liberamos la estructura
                free(query_recibida->archivo_query);
                free(query_recibida);
                break;
            }
            
            // --- DESALOJO POR PRIORIDAD ---
            case DESALOJO_PRIORIDADES: {
                log_warning(logger_worker, "## Solicitud de DESALOJO por PRIORIDAD recibida. (No implementado)");
                // TODO: 
                // 1. Pausar la ejecución de `ejecutar_query` (requiere hilos)
                // 2. Obtener el PC actual.
                // 3. Serializar t_query_ejecucion con el PC actualizado.
                // 4. Enviar paquete DESALOJO_PRIORIDADES de vuelta al Master.
                break;
            }

            // --- DESCONEXIÓN DE QUERY CONTROL ---
            case DESCONEXION_QUERY: {
                log_warning(logger_worker, "## Solicitud de DESALOJO por DESCONEXIÓN recibida. (No implementado)");
                // TODO: 
                // 1. Pausar/Cancelar la ejecución de `ejecutar_query`
                // 2. Enviar paquete END al Master con motivo "DESCONEXION QUERY"
                break;
            }

            default:
                log_error(logger_worker, "Código de operación %d inesperado recibido del Master.", paquete_master->codigo_operacion);
                break;
        }

        liberar_paquete(paquete_master);
    }

    liberar_memoria();
    close(socket_master);
    close(socket_storage);
    log_info(logger_worker, "Worker %d finalizado.", id_worker);
    return 0;
}