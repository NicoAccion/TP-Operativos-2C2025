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

        if (paquete_query->codigo_operacion == PAQUETE_QUERY_EJECUCION) {
            uint32_t len;
            char* path_query = buffer_read_string(paquete_query->buffer, &len);
            
            // El Master debería enviar el ID en el paquete.
            int query_id_mock = 1; 
            log_info(logger_worker, "## Query %d: Se recibe la Query. El path de operaciones es: %s", query_id_mock, path_query);

            ejecutar_query(query_id_mock, path_query, socket_master, socket_storage);
            free(path_query);
        } else {
            log_warning(logger_worker, "Código de paquete inesperado desde Master: %d", paquete_query->codigo_operacion);
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

        // Preparamos la operación query en el stack
        t_operacion_query op;
        op.informacion = strdup(""); // Inicializar a string vacío
        op.file = strdup("");
        op.tag = strdup("");
        
        char* file_tag_str;
        char* file_tag_copy; // Copia para strtok

        if (strcmp(instruccion, "CREATE") == 0) {
            file_tag_str = strtok(NULL, " "); // "MATERIAS:BASE"
            
            if (file_tag_str) {
                file_tag_copy = strdup(file_tag_str);
                free(op.file); op.file = strdup(strtok(file_tag_copy, ":"));
                free(op.tag);  op.tag  = strdup(strtok(NULL, ":"));
                enviar_operacion_storage(socket_storage, CREATE, &op);
                free(file_tag_copy);
            }
        
        } else if (strcmp(instruccion, "TRUNCATE") == 0) {
            file_tag_str = strtok(NULL, " "); // "MATERIAS:BASE"
            char* tamanio_str = strtok(NULL, " "); // "1024"
            
            if (file_tag_str && tamanio_str) {
                file_tag_copy = strdup(file_tag_str);
                free(op.informacion); op.informacion = strdup(tamanio_str); // Guardamos el tamaño
                free(op.file); op.file = strdup(strtok(file_tag_copy, ":"));
                free(op.tag);  op.tag  = strdup(strtok(NULL, ":"));
                enviar_operacion_storage(socket_storage, TRUNCATE, &op);
                free(file_tag_copy);
            }
        
        } else if (strcmp(instruccion, "TAG") == 0) {
            char* file_tag_origen_str = strtok(NULL, " ");
            char* file_tag_destino_str = strtok(NULL, " ");
            
            if(file_tag_origen_str && file_tag_destino_str) {
                file_tag_copy = strdup(file_tag_origen_str);
                // Re-utilizamos la struct: 'file:tag' para origen, 'informacion' para destino
                free(op.informacion); op.informacion = strdup(file_tag_destino_str);
                free(op.file); op.file = strdup(strtok(file_tag_copy, ":"));
                free(op.tag);  op.tag  = strdup(strtok(NULL, ":"));
                enviar_operacion_storage(socket_storage, TAG, &op);
                free(file_tag_copy);
            }

        } else if (strcmp(instruccion, "COMMIT") == 0) {
             file_tag_str = strtok(NULL, " ");
             
             if(file_tag_str) {
                file_tag_copy = strdup(file_tag_str);
                free(op.file); op.file = strdup(strtok(file_tag_copy, ":"));
                free(op.tag);  op.tag  = strdup(strtok(NULL, ":"));
                enviar_operacion_storage(socket_storage, COMMIT, &op);
                free(file_tag_copy);
             }

        } else if (strcmp(instruccion, "DELETE") == 0) {
            file_tag_str = strtok(NULL, " ");
            
            if(file_tag_str) {
                file_tag_copy = strdup(file_tag_str);
                free(op.file); op.file = strdup(strtok(file_tag_copy, ":"));
                free(op.tag);  op.tag  = strdup(strtok(NULL, ":"));
                enviar_operacion_storage(socket_storage, DELETE, &op);
                free(file_tag_copy);
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

void enviar_operacion_storage(int socket_storage, t_codigo_operacion op_code, t_operacion_query* op_query) {
    
    t_buffer* buffer = serializar_operacion_query(op_query);
    t_paquete* paquete = empaquetar_buffer(op_code, buffer);
    enviar_paquete(socket_storage, paquete);

    // Esperamos respuesta 
    t_paquete* paquete_rta = recibir_paquete(socket_storage);
    if (paquete_rta == NULL) {
        log_error(logger_worker, "Storage se desconectó inesperadamente.");
        //Se podria manejar errores abruptos
    } else {
        log_info(logger_worker, "Recibida confirmación del Storage (op_code: %d)", paquete_rta->codigo_operacion); //logeamos la respuesta segun el valor del code_op en utils
        liberar_paquete(paquete_rta);
    }
}