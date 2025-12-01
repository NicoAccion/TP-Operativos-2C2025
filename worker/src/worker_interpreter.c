#include "worker_interpreter.h"
#include "worker_memoria.h"
#include "worker.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

// --- Variables Globales --- 
uint32_t query_actual_id = 0; 
uint32_t query_actual_pc = 0; 
bool ejecutando_query = false; 
bool desalojar_actual = false; 
bool desconexion_actual = false; 
bool error = false;

// --- Sincronización ---
pthread_mutex_t mutex_flags;

void inicializar_estructuras_globales() {

    query_actual_id = 0;
    query_actual_pc = 0;
    ejecutando_query = false;
    desalojar_actual = false;
    desconexion_actual = false;
    error = false;

    pthread_mutex_init(&mutex_flags, NULL);
}

void destruir_estructuras_globales() {

    pthread_mutex_destroy(&mutex_flags);
}


/**
 * @brief Envía una operación simple (CREATE, WRITE, TRUNCATE, etc.) y espera una respuesta OK/ERROR.
 */
t_codigo_operacion enviar_op_simple_storage(int socket_storage, int socket_master, t_codigo_operacion op_code, t_op_storage* op) {
    t_buffer* buffer = serializar_op_storage(op, op_code);
    t_paquete* paquete = empaquetar_buffer(op_code, buffer);
    enviar_paquete(socket_storage, paquete);
    destruir_op_storage(op);

    t_paquete* paquete_rta = recibir_paquete(socket_storage);
    if (paquete_rta == NULL) {
        log_error(logger_worker, "Storage se desconectó inesperadamente.");
        return OP_ERROR;
    }
    t_codigo_operacion rta_code = paquete_rta->codigo_operacion;

    t_buffer* buffer_error = NULL;
    t_paquete* paquete_error = NULL;
    
    switch(rta_code){

        case OP_OK:
            log_info(logger_worker, "Recibida confirmación del Storage (op_code: %d)", rta_code);
            break;

        case FILE_TAG_INEXISTENTE:
            buffer_error = serializar_operacion_end("FILE / TAG INEXISTENTE");
            paquete_error = empaquetar_buffer(END, buffer_error);
            enviar_paquete(socket_master, paquete_error);
            error = true;
            break;

        case FILE_TAG_PREEXISTENTE:
            buffer_error = serializar_operacion_end("FILE / TAG PREEXISTENTE");
            paquete_error = empaquetar_buffer(END, buffer_error);
            enviar_paquete(socket_master, paquete_error);
            error = true;
            break;

        case ESPACIO_INSUFICIENTE:
            buffer_error = serializar_operacion_end("ESPACIO INSUFICIENTE");
            paquete_error = empaquetar_buffer(END, buffer_error);
            enviar_paquete(socket_master, paquete_error);
            error = true;
            break;

        case ESCRITURA_NO_PERMITIDA:
            buffer_error = serializar_operacion_end("ESCRITURA NO PERMITIDA");
            paquete_error = empaquetar_buffer(END, buffer_error);
            enviar_paquete(socket_master, paquete_error);
            error = true;
            break;

        case LECTURA_O_ESCRITURA_FUERA_DE_LIMITE:
            buffer_error = serializar_operacion_end("LECTURA O ESCRITURA FUERA DE LIMITE");
            paquete_error = empaquetar_buffer(END, buffer_error);
            enviar_paquete(socket_master, paquete_error);
            error = true;
            break;

        default:
            log_warning(logger_worker, "## Código de operación inesperado (%d)", rta_code);
            break;
    }
    
    liberar_paquete(paquete_rta);
    return rta_code;
}

/**
 * @brief Envía una operación READ al Storage y espera un paquete READ_RTA con el contenido.
 * @return El contenido leído (char*), o NULL si falló.
 */
char* enviar_op_read_storage(int socket_storage, t_op_storage* op) {
    t_buffer* buffer = serializar_op_storage(op, READ);
    t_paquete* paquete = empaquetar_buffer(READ, buffer);
    enviar_paquete(socket_storage, paquete);
    destruir_op_storage(op);

    t_paquete* paquete_rta = recibir_paquete(socket_storage);
    if (paquete_rta == NULL) {
        log_error(logger_worker, "Storage se desconectó (esperando READ_RTA).");
        return NULL;
    }
    if (paquete_rta->codigo_operacion != READ_RTA) {
        log_error(logger_worker, "Error del Storage: se esperaba READ_RTA (op_code: %d)", paquete_rta->codigo_operacion);
        liberar_paquete(paquete_rta);
        return NULL;
    }
    t_op_storage* op_rta = deserializar_op_storage(paquete_rta->buffer, READ_RTA); 
    char* contenido = malloc(op_rta->tamano_contenido);
    memcpy(contenido, op_rta->contenido, op_rta->tamano_contenido);
    destruir_op_storage(op_rta);
    liberar_paquete(paquete_rta);
    return contenido;
}

void ejecutar_query(int query_id, char* path_query, uint32_t program_counter,
                    int socket_master, int socket_storage) {
    
    FILE* archivo = fopen(path_query, "r");
    if (!archivo) {
        log_error(logger_worker, "No se pudo abrir el archivo de Query: %s", path_query);
        t_buffer* buffer_end = serializar_operacion_end("ERROR_APERTURA");
        t_paquete* paquete_end = empaquetar_buffer(END, buffer_end);
        enviar_paquete(socket_master, paquete_end);
        return;
    }


    // Inicializo estado global
    pthread_mutex_lock(&mutex_flags);
    query_actual_id = query_id;
    query_actual_pc = 0;
    ejecutando_query = true;
    desalojar_actual = false;
    desconexion_actual = false;
    error = false;
    pthread_mutex_unlock(&mutex_flags);


    char linea[256];
    uint32_t pc_actual = 0;
    bool fin = false;

    // --- Lógica de Program Counter ---
    // Adelantamos el archivo hasta la línea que nos dijo el Master
    while (pc_actual < program_counter && fgets(linea, sizeof(linea), archivo)) {
        pc_actual++;
    }

    while (!fin && fgets(linea, sizeof(linea), archivo)) {

        pthread_mutex_lock(&mutex_flags);
        bool hay_desalojar = desalojar_actual;
        bool hay_desconexion = desconexion_actual;
        bool hay_error = error;
        pthread_mutex_unlock(&mutex_flags);

        if (hay_desalojar) {
            log_info(logger_worker, "## Query %d: Desalojo solicitado (PC=%d)", query_actual_id, pc_actual);

            // Antes de irse, guarda todo lo que esté "sucio" en memoria
            realizar_flush_file(query_actual_id, NULL, NULL, socket_storage, socket_master); 

            // Paquete con el Program Counter actual
            t_query_ejecucion query_actualizada = {path_query, query_id, pc_actual};
            t_buffer* buffer_pc = serializar_query_ejecucion(&query_actualizada);
            t_paquete* paquete_pc = empaquetar_buffer(DESALOJO_PRIORIDADES, buffer_pc);
            enviar_paquete(socket_master, paquete_pc);

            pthread_mutex_lock(&mutex_flags);
            desalojar_actual = false;
            desconexion_actual = false;
            error = false;
            ejecutando_query = false;
            query_actual_id = 0;
            query_actual_pc = 0;
            pthread_mutex_unlock(&mutex_flags);

            fclose(archivo);
            return;
        }

        if (hay_desconexion) {

            log_info(logger_worker, "Se desconectó la query, %d", query_actual_id);
            t_buffer* buffer = serializar_operacion_end("DESCONEXION QUERY");
            t_paquete* paquete = empaquetar_buffer(END, buffer);
            enviar_paquete(socket_master, paquete);

            pthread_mutex_lock(&mutex_flags);
            desalojar_actual = false;
            desconexion_actual = false;
            error = false;
            ejecutando_query = false;
            query_actual_id = 0;
            query_actual_pc = 0;
            pthread_mutex_unlock(&mutex_flags);

            fclose(archivo);
            return;
        }

        if (hay_error){
            
            pthread_mutex_lock(&mutex_flags);
            desalojar_actual = false;
            desconexion_actual = false;
            error = false;
            ejecutando_query = false;
            query_actual_id = 0;
            query_actual_pc = 0;
            pthread_mutex_unlock(&mutex_flags);

            fclose(archivo);
            return;
        }

        linea[strcspn(linea, "\r\n")] = 0; 
        if (strlen(linea) == 0) continue; 

        char* linea_copy = strdup(linea); 
        char* instruccion = strtok(linea, " ");
        
        if (!instruccion) { 
            free(linea_copy);
            continue;
        }

        log_info(logger_worker, "## Query %d: FETCH - Program Counter: %d - %s", query_id, pc_actual, instruccion);
        usleep(worker_configs.retardomemoria * 1000);
        
        char* file_tag_str;
        char* file_tag_copy;

        // ==== CREATE ====
        if (strcmp(instruccion, "CREATE") == 0) {
            file_tag_str = strtok(NULL, " "); 
            if (file_tag_str) {
                t_op_storage* op_create = calloc(1, sizeof(t_op_storage));
                op_create->query_id = query_id; 
                file_tag_copy = strdup(file_tag_str);
                op_create->nombre_file = strdup(strtok(file_tag_copy, ":"));
                op_create->nombre_tag  = strdup(strtok(NULL, ":"));
                enviar_op_simple_storage(socket_storage, socket_master, CREATE, op_create);
                free(file_tag_copy);
            }
        }
        // ==== TRUNCATE ====
        else if (strcmp(instruccion, "TRUNCATE") == 0) {
            file_tag_str = strtok(NULL, " "); 
            char* tamanio_str = strtok(NULL, " "); 
            if (file_tag_str && tamanio_str) {
                t_op_storage* op_truncate = calloc(1, sizeof(t_op_storage));
                op_truncate->query_id = query_id;
                file_tag_copy = strdup(file_tag_str);
                op_truncate->nombre_file = strdup(strtok(file_tag_copy, ":"));
                op_truncate->nombre_tag  = strdup(strtok(NULL, ":"));
                op_truncate->tamano = atoi(tamanio_str);
                enviar_op_simple_storage(socket_storage, socket_master, TRUNCATE, op_truncate);
                free(file_tag_copy);
            }
        }
        // ==== WRITE ====
        else if (strcmp(instruccion, "WRITE") == 0) {
            char* file_tag = strtok(NULL, " ");
            char* direccion = strtok(NULL, " ");
            char* contenido = strtok(NULL, ""); 
            
            if(file_tag && direccion && contenido) {
                // Parseamos localmente para llamar a la memoria
                file_tag_copy = strdup(file_tag);
                char* file_local = strdup(strtok(file_tag_copy, ":"));
                char* tag_local  = strdup(strtok(NULL, ":"));
                
                // LLAMADA A LA MMU 
                escribir_en_memoria(query_id, file_local, tag_local, atoi(direccion), contenido, socket_storage, socket_master);
                
                free(file_local);
                free(tag_local);
                free(file_tag_copy);
            }
        }
        // ==== READ ====
        else if (strcmp(instruccion, "READ") == 0) {
            char* file_tag = strtok(NULL, " ");
            char* direccion = strtok(NULL, " ");
            char* tamanio = strtok(NULL, " ");
            
            if(file_tag && direccion && tamanio) {
                file_tag_copy = strdup(file_tag);
                char* file_local = strdup(strtok(file_tag_copy, ":"));
                char* tag_local  = strdup(strtok(NULL, ":"));

                // LLAMADA A LA MMU. Si hay Page Fault, mmu deberia encargarse
                char* valor_leido = leer_de_memoria(query_id, file_local, tag_local, 
                                                    atoi(direccion), atoi(tamanio), socket_storage, socket_master);
                
                if (valor_leido != NULL) {
                    t_operacion_query op_read_rta;
                    op_read_rta.file = strdup(file_local); 
                    op_read_rta.tag = strdup(tag_local);   
                    op_read_rta.informacion = valor_leido;
                    
                    t_buffer* buffer_read = serializar_operacion_query(&op_read_rta);
                    t_paquete* paquete_read = empaquetar_buffer(READ, buffer_read);
                    enviar_paquete(socket_master, paquete_read);

                    free(op_read_rta.file);
                    free(op_read_rta.tag);
                    // valor_leido se libera aqui
                    free(op_read_rta.informacion); 
                }
                free(file_local);
                free(tag_local);
                free(file_tag_copy);
            }
        }
        // ==== TAG ====
        else if (strcmp(instruccion, "TAG") == 0) {
            char* file_tag_origen_str = strtok(NULL, " ");
            char* file_tag_destino_str = strtok(NULL, " ");
            if(file_tag_origen_str && file_tag_destino_str) {
                t_op_storage* op_tag = calloc(1, sizeof(t_op_storage));
                op_tag->query_id = query_id;
                file_tag_copy = strdup(file_tag_origen_str);
                op_tag->nombre_file = strdup(strtok(file_tag_copy, ":"));
                op_tag->nombre_tag  = strdup(strtok(NULL, ":"));
                free(file_tag_copy);
                file_tag_copy = strdup(file_tag_destino_str);
                op_tag->nombre_file_destino = strdup(strtok(file_tag_copy, ":"));
                op_tag->nombre_tag_destino  = strdup(strtok(NULL, ":"));
                free(file_tag_copy);
                enviar_op_simple_storage(socket_storage, socket_master, TAG, op_tag);
            }
        }

        // ==== COMMIT ====
        else if (strcmp(instruccion, "COMMIT") == 0) {
            file_tag_str = strtok(NULL, " ");
            if(file_tag_str) {
                file_tag_copy = strdup(file_tag_str);
                char* file_local = strdup(strtok(file_tag_copy, ":"));
                char* tag_local  = strdup(strtok(NULL, ":"));
                
                // 1. FLUSH de páginas sucias de ESTE archivo
                realizar_flush_file(query_id, file_local, tag_local, socket_storage, socket_master);
                
                // 2. Enviar instrucción COMMIT
                t_op_storage* op_commit = calloc(1, sizeof(t_op_storage));
                op_commit->query_id = query_id;
                op_commit->nombre_file = strdup(file_local);
                op_commit->nombre_tag  = strdup(tag_local);
                
                enviar_op_simple_storage(socket_storage, socket_master, COMMIT, op_commit);
                
                free(file_local);
                free(tag_local);
                free(file_tag_copy);
             }
        }
        // ==== FLUSH ====
        else if (strcmp(instruccion, "FLUSH") == 0) {
            
             file_tag_str = strtok(NULL, " ");
             if(file_tag_str) {
                file_tag_copy = strdup(file_tag_str);
                char* file_local = strdup(strtok(file_tag_copy, ":"));
                char* tag_local  = strdup(strtok(NULL, ":"));
                realizar_flush_file(query_id, file_local, tag_local, socket_storage, socket_master);
                free(file_local);
                free(tag_local);
                free(file_tag_copy);
             }
        }
        // ==== DELETE ====
        else if (strcmp(instruccion, "DELETE") == 0) {
            file_tag_str = strtok(NULL, " ");
            if(file_tag_str) {
                t_op_storage* op_delete = calloc(1, sizeof(t_op_storage));
                op_delete->query_id = query_id;
                file_tag_copy = strdup(file_tag_str);
                op_delete->nombre_file = strdup(strtok(file_tag_copy, ":"));
                op_delete->nombre_tag  = strdup(strtok(NULL, ":"));
                free(file_tag_copy);
                enviar_op_simple_storage(socket_storage, socket_master, DELETE, op_delete);
            }
        }
        // ==== END ====
        else if (strcmp(instruccion, "END") == 0) {
            char* motivo = "OK";
            t_buffer* buffer_end = serializar_operacion_end(motivo);
            t_paquete* paquete_end = empaquetar_buffer(END, buffer_end);
            enviar_paquete(socket_master, paquete_end);
            fin = true;
        }

        //esperar_retardo();
        log_info(logger_worker, "## Query %d: - Instrucción realizada: %s", query_id, linea_copy);
        free(linea_copy); 
        pc_actual++;
    }

    fclose(archivo);

    pthread_mutex_lock(&mutex_flags);
    ejecutando_query = false;
    desalojar_actual = false;
    error = false;
    query_actual_id = 0;
    query_actual_pc = 0;
    pthread_mutex_unlock(&mutex_flags);
}
