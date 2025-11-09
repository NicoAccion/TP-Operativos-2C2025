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
                
                query_actual_id = query_recibida->id_query;
                query_actual_pc = query_recibida->program_counter;
                desalojar_actual = false;
                ejecutando_query = true;

                // 3. Llamamos al interpeter
                ejecutar_query(query_recibida->id_query, 
                               query_recibida->archivo_query, 
                               query_recibida->program_counter, // Pasamos el PC
                               socket_master, 
                               socket_storage);

                ejecutando_query = false;
                query_actual_id = 0;
                query_actual_pc = 0;
                desalojar_actual = false;
                
                // 4. Liberamos la estructura
                free(query_recibida->archivo_query);
                free(query_recibida);
                break;
            }
            
            // --- DESALOJO POR PRIORIDAD ---
            case DESALOJO_PRIORIDADES: {
                /*   
                Las Queries se enviarán a ejecutar según su prioridad, siendo 0 la prioridad más alta (a mayor número, menor prioridad). 
                En caso que los Workers estuvieran ocupados y una Query nueva tuviera más prioridad que alguna de las que estuviera en ejecución, deberá ser desalojada aquella que tuviera la menor prioridad.
                Para desalojar una Query de un Worker, se deberá solicitar al Worker su desalojo. Este último le devolverá al Master el Program Counter (PC) para que éste luego sepa desde dónde reanudarlo. 
                Al momento de volver a planificar una Query previamente desalojada, se deberá enviar el PC previamente recibido para poder retomar en el lugar correcto.
                */

                // 1. Pausar la ejecución de `ejecutar_query`
                // 2. Obtener el PC actual.
                // 3. Serializar t_query_ejecucion con el PC actualizado.
                // 4. Enviar paquete DESALOJO_PRIORIDADES de vuelta al Master. 

                log_warning(logger_worker, "## Solicitud de DESALOJO por PRIORIDAD recibida.");

                // Deserializar el ID de la Query que el Master quiere desalojar
                uint32_t id_query_desalojar = buffer_read_uint32(paquete_master->buffer);
                log_info(logger_worker, "DESALOJO_PRIORIDADES pedido para Query %d (estado local: ejecutando=%d id=%d)", 
                         id_query_desalojar, ejecutando_query, query_actual_id);

                if (ejecutando_query && id_query_desalojar == query_actual_id) {
                    // Marcamos señal para que ejecutar_query corte limpiamente y envíe el contexto
                    desalojar_actual = true;
                    log_info(logger_worker, "Marcado desalojar_actual = true para Query %d", id_query_desalojar);
                    

                }

                break;
            }

            // --- DESCONEXIÓN DE QUERY CONTROL ---
            case DESCONEXION_QUERY: {
                /*
                Ante la desconexión de un módulo Query Control, la Query enviada por el mismo deberá cancelarse.
                En el caso de que la Query se encuentre en READY, la misma se deberá enviar a EXIT directamente.
                En caso de que la Query se encuentre en EXEC, previamente se deberá notificar al Worker que la está
                ejecutando que debe desalojar dicha Query. Una vez recibido su contexto se enviará la Query a EXIT.
                */

                log_warning(logger_worker, "## Solicitud de DESALOJO por DESCONEXIÓN recibida.");

                // 1. Pausar/Cancelar la ejecución de `ejecutar_query`
                // 2. Enviar paquete END al Master con motivo "DESCONEXION QUERY"

                // Deserializar el ID de la query desconectada
                uint32_t id_query_desconectada = buffer_read_uint32(paquete_master->buffer);
                log_info(logger_worker, "Query %d desconectada por su Query Control.", id_query_desconectada);

                // Verificar si es la Query en ejecución
                if (ejecutando_query && id_query_desconectada == query_actual_id) {
                    log_info(logger_worker, "Cancelando ejecución actual de Query %d por desconexión.", id_query_desconectada);

                    desalojar_actual = true;
                    ejecutando_query = false;
                    query_actual_id = 0;
                    query_actual_pc = 0;

                } else {
                    log_info(logger_worker, "La Query %d no estaba en ejecución.", id_query_desconectada);
                }

                // Notificar al Master que la Query finalizó por desconexión
                t_buffer* buffer_end = serializar_operacion_end("DESCONEXION QUERY");
                t_paquete* paquete_end = empaquetar_buffer(END, buffer_end);
                enviar_paquete(socket_master, paquete_end);

                log_info(logger_worker, "Query %d deberá ser enviada a EXIT (motivo: desconexión del Query Control).", id_query_desconectada);

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