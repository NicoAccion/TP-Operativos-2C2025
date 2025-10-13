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

    // ===== Conexión con Storage (mock) =====
    char* puerto_storage_str = string_itoa(worker_configs.puertostorage);
    int socket_storage = crear_conexion(worker_configs.ipstorage, puerto_storage_str);
    free(puerto_storage_str);
    if (socket_storage < 0) {
        log_error(logger_worker, "Error al conectar con Storage");
        return EXIT_FAILURE;
    }
    log_info(logger_worker, "Conectado a Storage");
    int tamanio_bloque_mock = 128;
    inicializar_memoria(worker_configs.tammemoria, tamanio_bloque_mock);


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
        linea[strcspn(linea, "\r\n")] = 0;
        if (strlen(linea) == 0) continue;

        char* linea_copy = strdup(linea);
        char* instruccion = strtok(linea, " ");
        
        log_info(logger_worker, "## Query %d: FETCH Program Counter: %d - %s", query_id, pc, instruccion);
        usleep(worker_configs.retardomemoria * 1000);

        if (strcmp(instruccion, "CREATE") == 0) {
            char* file_tag = strtok(NULL, " ");
            enviar_operacion_storage_simple(socket_storage, CREATE, file_tag);
        
        } else if (strcmp(instruccion, "TRUNCATE") == 0) {
            char* file_tag = strtok(NULL, " ");
            char* tamanio = strtok(NULL, " ");
            enviar_operacion_storage_mock(socket_storage, TRUNCATE, file_tag, tamanio);
        
        } else if (strcmp(instruccion, "TAG") == 0) {
            char* file_tag_origen = strtok(NULL, " ");
            char* file_tag_destino = strtok(NULL, " ");
            enviar_operacion_storage_mock(socket_storage, TAG, file_tag_origen, file_tag_destino);

        } else if (strcmp(instruccion, "COMMIT") == 0) {
             char* file_tag = strtok(NULL, " ");
             enviar_operacion_storage_simple(socket_storage, COMMIT, file_tag);

        } else if (strcmp(instruccion, "DELETE") == 0) {
            char* file_tag = strtok(NULL, " ");
            enviar_operacion_storage_simple(socket_storage, DELETE, file_tag);

        } else if (strcmp(instruccion, "WRITE") == 0) {
            char* file_tag = strtok(NULL, " ");
            char* direccion = strtok(NULL, " ");
            char* contenido = strtok(NULL, ""); 
            char* file_tag_copy = strdup(file_tag);
            char* file = strtok(file_tag_copy, ":");
            char* tag = strtok(NULL, ":");
            
            escribir_en_memoria(query_id, file, tag, atoi(direccion), contenido);
            free(file_tag_copy);

        } else if (strcmp(instruccion, "READ") == 0) {
            char* file_tag = strtok(NULL, " ");
            char* direccion = strtok(NULL, " ");
            char* tamanio = strtok(NULL, " ");
            char* file_tag_copy = strdup(file_tag);
            char* file = strtok(file_tag_copy, ":");
            char* tag = strtok(NULL, ":");
            
            char* valor_leido = leer_de_memoria(query_id, file, tag, atoi(direccion), atoi(tamanio));
            
            t_operacion_query op = {.file = file, .tag = tag, .informacion = valor_leido};
            t_buffer* buffer_read = serializar_operacion_query(&op);
            t_paquete* paquete_read = empaquetar_buffer(READ, buffer_read);
            enviar_paquete(socket_master, paquete_read);

            free(valor_leido);
            free(file_tag_copy);

        } else if (strcmp(instruccion, "FLUSH") == 0) {
            // FLUSH es una operación local de memoria, no necesita ir a Storage en este punto.
            // La lógica real escribiría las páginas sucias de memoria a disco.
        } else if (strcmp(instruccion, "END") == 0) {
            char* motivo = "OK";
            t_buffer* buffer_end = buffer_create(sizeof(uint32_t) + strlen(motivo));
            buffer_add_string(buffer_end, strlen(motivo), motivo);
            t_paquete* paquete_end = empaquetar_buffer(END, buffer_end);
            enviar_paquete(socket_master, paquete_end);
            fin = true;
        }
        
        log_info(logger_worker, "## Query %d: Instrucción realizada: %s", query_id, linea_copy);
        free(linea_copy);
        pc++;
    }

    fclose(archivo);
}


// --- Funciones Auxiliares para Comunicación con Storage---

void enviar_operacion_storage_mock(int socket_storage, t_codigo_operacion op_code, const char* arg1, const char* arg2) {
    // Usamos funciones de utils
    size_t size = sizeof(uint32_t) + strlen(arg1) + sizeof(uint32_t) + strlen(arg2);
    t_buffer* buffer = buffer_create(size);
    buffer_add_string(buffer, strlen(arg1), (char*) arg1);
    buffer_add_string(buffer, strlen(arg2), (char*) arg2);

    t_paquete* paquete = empaquetar_buffer(op_code, buffer);
    enviar_paquete(socket_storage, paquete);

    // Esperamos respuesta
    liberar_paquete(recibir_paquete(socket_storage));
}

void enviar_operacion_storage_simple(int socket_storage, t_codigo_operacion op_code, const char* arg1) {
    size_t size = sizeof(uint32_t) + strlen(arg1);
    t_buffer* buffer = buffer_create(size);
    buffer_add_string(buffer, strlen(arg1), (char*) arg1);

    t_paquete* paquete = empaquetar_buffer(op_code, buffer);
    enviar_paquete(socket_storage, paquete);

    liberar_paquete(recibir_paquete(socket_storage));
}