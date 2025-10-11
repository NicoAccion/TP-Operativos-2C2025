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

    // Simular comportamiento según instrucción
    if (strncmp(instruccion, "READ", 4) == 0) {
        // Enviar lectura al Master
        char mensaje[256];
        snprintf(mensaje, sizeof(mensaje), "READ|Query:%d|Instruccion:%s", query_id, instruccion);
        enviar_mensaje(mensaje, socket_master);
        // --- LOG: Query enviada a Master ---
        log_info(logger_worker, "## Query %d: Resultado de lectura enviado al Master", query_id);
    }

    if (strncmp(instruccion, "END", 3) == 0) {
        // Avisar fin de Query al Master
        char mensaje[64];
        snprintf(mensaje, sizeof(mensaje), "END|Query:%d", query_id);
        enviar_mensaje(mensaje, socket_master);
        // --- LOG: Query finalizada ---
        log_info(logger_worker, "## Query %d: Finalizada ejecución (END)", query_id);
    }
}

// Ejecuta el archivo línea por línea
void ejecutar_query(const char* path_query, int socket_master, int socket_storage, int query_id) {
    FILE* archivo = fopen(path_query, "r");
    if (!archivo) {
        // --- LOG: No se pudo abrir el archivo ---
        log_error(logger_worker, "❌ No se pudo abrir el archivo de Query: %s", path_query);
        return;
    }

    // --- LOG: Query recibida ---
    log_info(logger_worker, "## Query %d: Se recibe la Query. Path de operaciones: %s", query_id, path_query);

    char linea[256];
    int pc = 0;

    while (fgets(linea, sizeof(linea), archivo)) {
        // Eliminar salto de línea
        linea[strcspn(linea, "\r\n")] = 0;
        if (strlen(linea) == 0) continue; // Ignorar líneas vacías

        ejecutar_instruccion(linea, query_id, pc, socket_master);

        if (strncmp(linea, "END", 3) == 0) {
            break; // Terminar ejecución
        }

        pc++;
    }

    fclose(archivo);
}

int main(int argc, char* argv[]) {

    // Validar argumentos
    if (argc != 3) {
        fprintf(stderr, "Uso: %s [archivo_config] [ID worker]\n", argv[0]);
        return EXIT_FAILURE;
    }

    char* path_config = argv[1];
    char* id_worker = argv[2];

    saludar("worker");

    // Inicializo configuración y logger
    inicializar_configs(path_config);
    inicializar_logger_worker(worker_configs.loglevel);

    // --- LOG: Worker iniciado ---
    log_info(logger_worker, "## Worker %s inicializado con loglevel %d", id_worker, worker_configs.loglevel);

    // Conexión con Storage
    // =============================
    int socket_storage = crear_conexion(worker_configs.ipstorage, string_itoa(worker_configs.puertostorage));
    if (socket_storage < 0) {
        fprintf(stderr, "Error al conectar con Storage\n");
        return EXIT_FAILURE;
    }

    // --- LOG: Storage conectado ---
    log_info(logger_worker, "Conectado a Storage (%s:%d)", worker_configs.ipstorage, worker_configs.puertostorage);

    // Handshake con Storage: solicitar tamaño de bloque
    enviar_mensaje("HANDSHAKE_WORKER", socket_storage);

    char* tam_bloque = recibir_mensaje(socket_storage);
    if (tam_bloque != NULL) {
        // --- LOG: Bloque recibido ---
        log_info(logger_worker, "Tamaño de bloque recibido de Storage: %s bytes", tam_bloque);
        worker_configs.tamanio_bloque = atoi(tam_bloque);
        free(tam_bloque);
    } else {
        // --- LOG: Error al recibir bloque ---
        log_error(logger_worker, "Error al recibir tamaño de bloque de Storage");
        close(socket_storage);
        return EXIT_FAILURE;
    }

    inicializar_memoria(worker_configs.tammemoria, atoi(tam_bloque));


    // Conexión con Master
    int socket_master = crear_conexion(worker_configs.ipmaster, string_itoa(worker_configs.puertomaster));
    if (socket_master < 0) {
        // --- LOG: Error al conectar con Master ---
        log_error(logger_worker, "Error al conectar con Master (%s:%d)", worker_configs.ipmaster, worker_configs.puertomaster);
        close(socket_storage);
        return EXIT_FAILURE;
    }

    // --- LOG: Master conectado ---
    log_info(logger_worker, "Conectado a Master (%s:%d)", worker_configs.ipmaster, worker_configs.puertomaster);

    // Enviar ID del Worker al Master
    enviar_mensaje(id_worker, socket_master);

    inicializar_memoria(worker_configs.tammemoria, worker_configs.tamanio_bloque);

    // Esperar path del Query desde Master
    char* path_query = recibir_mensaje(socket_master);
    if (path_query != NULL) {

        // --- LOG: Query recibida---
        log_info(logger_worker, "Query recibida: %s", path_query);

        int query_id = atoi(id_worker); // Por ahora usamos el ID del worker como query_id simulado
        ejecutar_query(path_query, socket_master, socket_storage, query_id);

        free(path_query);
    } else {
        // --- LOG: Error al recibir Query ---
        log_error(logger_worker, "Error al recibir Query del Master");
    }

    ejecutar_query(path_query, socket_master, socket_storage, id_worker);



    liberar_memoria();
    


    //    Cierre
    // =============================
    close(socket_master);
    close(socket_storage);

    // --- LOG: Worker finalizado ---
    log_info(logger_worker, "Worker %s finalizado correctamente.", id_worker);
    return 0;
}
