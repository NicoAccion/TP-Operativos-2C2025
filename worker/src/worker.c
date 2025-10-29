#include "worker.h"
#include "worker_memoria.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// --- Prototipos ---
void ejecutar_query(int query_id, const char* path_query, int socket_master, int socket_storage);
void enviar_operacion_storage_mock(int socket_storage, t_codigo_operacion op_code, const char* arg1, const char* arg2);
void enviar_operacion_storage_simple(int socket_storage, t_codigo_operacion op_code, const char* arg1);
t_codigo_operacion enviar_op_a_storage(int socket_storage, t_codigo_operacion op_code, t_op_storage* op);


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
            t_paquete* paquete_query = recibir_paquete(socket_master); 

            if (!paquete_query) {
                log_error(logger_worker, "Se perdió la conexión con Master.");
                break;
            }
            if (paquete_query->codigo_operacion == PAQUETE_QUERY_COMPLETA) {
            
            // 1. Deserializamos el paquete COMPLETO
            t_query_completa* query_recibida = deserializar_query_completa(paquete_query->buffer);

            // 2. Usamos el ID y path REALES
            log_info(logger_worker, "## Query %d: Se recibe la Query. El path de operaciones es: %s", 
                     query_recibida->id_query, query_recibida->archivo_query);

            // 3. Ejecutamos usando el id_query real
            ejecutar_query(query_recibida->id_query, 
                           query_recibida->archivo_query, 
                           socket_master, 
                           socket_storage);
            
            // 4. Liberamos la estructura
            free(query_recibida->archivo_query);
            free(query_recibida);
        }
        liberar_paquete(paquete_query);
    }

    liberar_memoria();
    close(socket_master);
    close(socket_storage);
    log_info(logger_worker, "Worker %d finalizado.", id_worker);
    return 0;
}

void ejecutar_query(int query_id, const char* path_query, int socket_master, int socket_storage) {
    FILE* archivo = fopen(path_query, "r");
    if (!archivo) {
        log_error(logger_worker, "No se pudo abrir el archivo de Query: %s", path_query);
        return;
    }

    char linea[256];
    int pc = 0;
    bool fin = false;

    while (!fin && fgets(linea, sizeof(linea), archivo)) {
        linea[strcspn(linea, "\r\n")] = 0; // Limpia saltos de línea
        if (strlen(linea) == 0) continue; // Salta líneas vacías

        char* linea_copy = strdup(linea); // Copia para el log final
        char* instruccion = strtok(linea, " ");
        
        if (!instruccion) { // Maneja líneas con solo espacios
            free(linea_copy);
            continue;
        }

        log_info(logger_worker, "## Query %d: FETCH Program Counter: %d - %s", query_id, pc, instruccion);
        usleep(worker_configs.retardomemoria * 1000); // Simula retardo
        
        char* file_tag_str;
        char* file_tag_copy; // Copia para strtok

if (strcmp(instruccion, "CREATE") == 0) {
            file_tag_str = strtok(NULL, " "); // "MATERIAS:BASE"
            
            if (file_tag_str) {
                // 1. Creamos la struct t_op_storage
                t_op_storage* op_create = calloc(1, sizeof(t_op_storage));
                op_create->query_id = query_id; // <-- Usamos el ID real

                // 2. Parseamos y poblamos
                file_tag_copy = strdup(file_tag_str);
                op_create->nombre_file = strdup(strtok(file_tag_copy, ":"));
                op_create->nombre_tag  = strdup(strtok(NULL, ":"));

                // 3. Llamamos a la nueva función
                enviar_op_a_storage(socket_storage, CREATE, op_create);
                // (op_create se libera dentro de la función)
                
                free(file_tag_copy);
            }
        
        } else if (strcmp(instruccion, "TRUNCATE") == 0) {
            file_tag_str = strtok(NULL, " "); // "MATERIAS:BASE"
            char* tamanio_str = strtok(NULL, " "); // "1024"
            
            if (file_tag_str && tamanio_str) {
                // 1. Creamos la struct
                t_op_storage* op_truncate = calloc(1, sizeof(t_op_storage));
                op_truncate->query_id = query_id;

                // 2. Parseamos y poblamos
                file_tag_copy = strdup(file_tag_str);
                op_truncate->nombre_file = strdup(strtok(file_tag_copy, ":"));
                op_truncate->nombre_tag  = strdup(strtok(NULL, ":"));
                op_truncate->tamano = atoi(tamanio_str); // Convertimos a int

                // 3. Llamamos
                enviar_op_a_storage(socket_storage, TRUNCATE, op_truncate);

                free(file_tag_copy);
            }
        
        } else if (strcmp(instruccion, "TAG") == 0) {
            char* file_tag_origen_str = strtok(NULL, " ");
            char* file_tag_destino_str = strtok(NULL, " ");
            
            if(file_tag_origen_str && file_tag_destino_str) {
                t_op_storage* op_tag = calloc(1, sizeof(t_op_storage));
                op_tag->query_id = query_id;
                
                // Origen
                file_tag_copy = strdup(file_tag_origen_str);
                op_tag->nombre_file = strdup(strtok(file_tag_copy, ":"));
                op_tag->nombre_tag  = strdup(strtok(NULL, ":"));
                free(file_tag_copy);

                // Destino
                file_tag_copy = strdup(file_tag_destino_str);
                op_tag->nombre_file_destino = strdup(strtok(file_tag_copy, ":"));
                op_tag->nombre_tag_destino  = strdup(strtok(NULL, ":"));
                free(file_tag_copy);

                enviar_op_a_storage(socket_storage, TAG, op_tag);
            }

        } else if (strcmp(instruccion, "COMMIT") == 0) {
             file_tag_str = strtok(NULL, " ");
             
             if(file_tag_str) {
                t_op_storage* op_commit = calloc(1, sizeof(t_op_storage));
                op_commit->query_id = query_id;
                
                file_tag_copy = strdup(file_tag_str);
                op_commit->nombre_file = strdup(strtok(file_tag_copy, ":"));
                op_commit->nombre_tag  = strdup(strtok(NULL, ":"));
                free(file_tag_copy);

                enviar_op_a_storage(socket_storage, COMMIT, op_commit);
             }

        } else if (strcmp(instruccion, "DELETE") == 0) {
            file_tag_str = strtok(NULL, " ");
            
            if(file_tag_str) {
                t_op_storage* op_delete = calloc(1, sizeof(t_op_storage));
                op_delete->query_id = query_id;

                file_tag_copy = strdup(file_tag_str);
                op_delete->nombre_file = strdup(strtok(file_tag_copy, ":"));
                op_delete->nombre_tag  = strdup(strtok(NULL, ":"));
                free(file_tag_copy);
                
                enviar_op_a_storage(socket_storage, DELETE, op_delete);
            }

        } else if (strcmp(instruccion, "WRITE") == 0) {
            // Aun no habla con storage con write
            char* file_tag = strtok(NULL, " ");
            char* direccion = strtok(NULL, " ");
            char* contenido = strtok(NULL, ""); // Captura el resto de la línea
            
            if(file_tag && direccion && contenido) {
                char* file_tag_copy = strdup(file_tag);
                char* file = strtok(file_tag_copy, ":");
                char* tag = strtok(NULL, ":");
                
                escribir_en_memoria(query_id, file, tag, atoi(direccion), contenido);
                free(file_tag_copy);
            }

        } else if (strcmp(instruccion, "READ") == 0) {
            //aun no habla con storage si hay read
            char* file_tag = strtok(NULL, " ");
            char* direccion = strtok(NULL, " ");
            char* tamanio = strtok(NULL, " ");
            
            if(file_tag && direccion && tamanio) {
                char* file_tag_copy = strdup(file_tag);
                char* file = strtok(file_tag_copy, ":");
                char* tag = strtok(NULL, ":");
                
                char* valor_leido = leer_de_memoria(query_id, file, tag, atoi(direccion), atoi(tamanio));
                
                // Usamos una struct temporal para enviar al Master
                t_operacion_query op_read = {.file = file, .tag = tag, .informacion = valor_leido};
                t_buffer* buffer_read = serializar_operacion_query(&op_read);
                t_paquete* paquete_read = empaquetar_buffer(READ, buffer_read);
                enviar_paquete(socket_master, paquete_read);

                free(valor_leido);
                free(file_tag_copy);
            }

        } else if (strcmp(instruccion, "FLUSH") == 0) {
            // Lógica de FLUSH 
        } else if (strcmp(instruccion, "END") == 0) {
            // Lógica de END
            char* motivo = "OK";
            t_buffer* buffer_end = serializar_operacion_end(motivo);
            t_paquete* paquete_end = empaquetar_buffer(END, buffer_end);
            enviar_paquete(socket_master, paquete_end);
            fin = true;
        }
        
        // Liberamos la memoria de los strings dentro de 'op'
        //destruir_operacion_query(&op); ME SALE DOBLE LIBERACION
        
        log_info(logger_worker, "## Query %d: Instrucción realizada: %s", query_id, linea_copy);
        free(linea_copy); // Liberamos la copia para el log
        pc++;
    }

    fclose(archivo);
}

t_codigo_operacion enviar_op_a_storage(int socket_storage, t_codigo_operacion op_code, t_op_storage* op) {
    
    // 1. Serializa usando la función CORRECTA
    t_buffer* buffer = serializar_op_storage(op, op_code);
    t_paquete* paquete = empaquetar_buffer(op_code, buffer);
    
    // 2. Enviamos el paquete
    enviar_paquete(socket_storage, paquete);

    // 3. Liberamos la estructura 'op' que creamos (ya está en el buffer)
    destruir_op_storage(op);

    // 4. Esperamos respuesta (OK/ERROR)
    t_paquete* paquete_rta = recibir_paquete(socket_storage);
    if (paquete_rta == NULL) {
        log_error(logger_worker, "Storage se desconectó inesperadamente.");
        return OP_ERROR; // Devolvemos error
    }

    t_codigo_operacion rta_code = paquete_rta->codigo_operacion;
    log_info(logger_worker, "Recibida confirmación del Storage (op_code: %d)", rta_code);
    liberar_paquete(paquete_rta);
    return rta_code;
}