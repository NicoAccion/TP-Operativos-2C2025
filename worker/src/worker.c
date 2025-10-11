#include "worker.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

// ===================== DECLARACIONES =====================

void ejecutar_query(const char* path_query, int socket_master, int socket_storage, int query_id);
void ejecutar_instruccion(const char* instruccion, int query_id, int pc, int socket_master);
void retardo_memoria();

// ===================== FUNCIONES =====================

// Simula el retardo de memoria configurado
void retardo_memoria() {
    usleep(worker_configs.retardo_memoria * 1000); // milisegundos → microsegundos
}

// Ejecuta una sola instrucción (mock)
void ejecutar_instruccion(const char* instruccion, int query_id, int pc, int socket_master) {
    // --- LOG: Ejecutar  Worker ---
    log_info(logger_worker, "## Query %d: FETCH - Program Counter: %d - %s", query_id, pc, instruccion);

    // Simular retardo
    retardo_memoria();

    // --- LOG: Query a ejecutar ---
    printf("## Query %d: - Instrucción realizada: %s\n", query_id, instruccion);

    if (strncmp(instruccion, "READ", 4) == 0) {
        // Serializamos una operación READ
        t_operacion_query op = {
            .file = strdup("file1.txt"),
            .tag = strdup("campo1"),
            .informacion = strdup(instruccion)
        };

        t_buffer* buffer = serializar_operacion_query(&op);
        t_paquete* paquete = empaquetar_buffer(READ, buffer);
        enviar_paquete(socket_master, paquete);

        destruir_operacion_query(&op);
        destruir_paquete(paquete);

        log_info(logger_worker, "## Query %d: Resultado de READ enviado al Master", query_id);
    }
    else if (strncmp(instruccion, "END", 3) == 0) {
        // Enviamos un END con motivo
        char* motivo = "Ejecución finalizada correctamente";
        t_buffer* buffer = serializar_string(motivo);
        t_paquete* paquete = empaquetar_buffer(END, buffer);
        enviar_paquete(socket_master, paquete);
        destruir_paquete(paquete);

        log_info(logger_worker, "## Query %d: Finalizada ejecución (END)", query_id);
    }
    else {
        log_info(logger_worker, "## Query %d: Instrucción ejecutada: %s", query_id, instruccion);
    }
}

// Ejecuta el archivo línea por línea
void ejecutar_query(const char* path_query, int socket_master, int socket_storage, int query_id) {
    FILE* archivo = fopen(path_query, "r");
    if (!archivo) {
        log_error(logger_worker, "No se pudo abrir el archivo de Query: %s", path_query);
        return;
    }

    log_info(logger_worker, "## Query %d: Path de operaciones: %s", query_id, path_query);

    char linea[256];
    int pc = 0;

    while (fgets(linea, sizeof(linea), archivo)) {
        linea[strcspn(linea, "\r\n")] = 0;
        if (strlen(linea) == 0) continue;

        ejecutar_instruccion(linea, query_id, pc, socket_master);

        if (strncmp(linea, "END", 3) == 0)
            break;

        pc++;
    }

    fclose(archivo);
}

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
    int socket_storage = crear_conexion(worker_configs.ipstorage,
                                        string_itoa(worker_configs.puertostorage));
    if (socket_storage < 0) {
        log_error(logger_worker, "Error al conectar con Storage");
        return EXIT_FAILURE;
    }

    log_info(logger_worker, "Conectado a Storage (%s:%d)",
             worker_configs.ipstorage, worker_configs.puertostorage);

    // Handshake con Storage
    t_buffer* buffer_hs = serializar_string("WORKER");
    t_paquete* paquete_hs = empaquetar_buffer(HANDSHAKE_WORKER, buffer_hs);
    enviar_paquete(socket_storage, paquete_hs);
    destruir_paquete(paquete_hs);

    // Esperar tamaño de bloque (respuesta)
    t_paquete* paquete_resp = recibir_paquete(socket_storage);
    if (paquete_resp && paquete_resp->codigo_operacion == TAMANIO_BLOQUE) {
        worker_configs.tamanio_bloque = deserializar_int(paquete_resp->buffer);
        log_info(logger_worker, "Tamaño de bloque recibido: %d bytes", worker_configs.tamanio_bloque);
        destruir_paquete(paquete_resp);
    } else {
        log_error(logger_worker, "Error al recibir tamaño de bloque");
        close(socket_storage);
        return EXIT_FAILURE;
    }

    inicializar_memoria(worker_configs.tammemoria, worker_configs.tamanio_bloque);

    // ===== Conexión con Master =====
    int socket_master = crear_conexion(worker_configs.ipmaster,
                                       string_itoa(worker_configs.puertomaster));
    if (socket_master < 0) {
        log_error(logger_worker, "Error al conectar con Master");
        close(socket_storage);
        return EXIT_FAILURE;
    }

    log_info(logger_worker, "Conectado a Master (%s:%d)",
             worker_configs.ipmaster, worker_configs.puertomaster);

    // Handshake con Master
    t_buffer* buffer_id = serializar_int(id_worker);
    t_paquete* paquete_w = empaquetar_buffer(HANDSHAKE_WORKER, buffer_id);
    enviar_paquete(socket_master, paquete_w);
    destruir_paquete(paquete_w);

    // ===== Esperar Query =====
    t_paquete* paquete_query = recibir_paquete(socket_master);
    if (!paquete_query) {
        log_error(logger_worker, "Error al recibir Query desde Master");
        close(socket_master);
        close(socket_storage);
        return EXIT_FAILURE;
    }

    if (paquete_query->codigo_operacion == PATH_QUERY) {
        char* path_query = deserializar_string(paquete_query->buffer);
        log_info(logger_worker, "Query recibida: %s", path_query);

        ejecutar_query(path_query, socket_master, socket_storage, id_worker);
        free(path_query);
    } else {
        log_warning(logger_worker, "Código inesperado de paquete desde Master: %d",
                    paquete_query->codigo_operacion);
    }

    destruir_paquete(paquete_query);

    liberar_memoria();
    close(socket_master);
    close(socket_storage);

    log_info(logger_worker, "Worker %d finalizado correctamente.", id_worker);
    return 0;
}
