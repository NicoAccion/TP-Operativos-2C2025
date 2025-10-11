// master_prioridades.c
#include "master.h"
#include <sys/select.h>
#include <unistd.h>
#include <stdbool.h>
#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>
#include <errno.h>

#define MAX_CLIENTES 200
#define MAX_QUERIES  200

typedef enum { CLIENTE_QUERY_CONTROL, CLIENTE_WORKER } TipoCliente;
typedef enum { READY, EXEC, EXIT } EstadoQuery;

typedef struct {
    int socket;
    TipoCliente tipo;
    int id;          // Worker id (si es worker) o -1
    bool activo;
    bool ocupado;    // solo para workers
} Cliente;

typedef struct {
    int id;
    int socket_query;    // socket del Query Control que envió esta Query
    int socket_worker;   // socket del Worker que la está ejecutando (o -1)
    char archivo_query[256];
    int prioridad;       // 0 = más alta
    EstadoQuery estado;
    int program_counter; // PC guardado al desalojar
    long long timestamp_entrada; // ms desde epoch para FIFO tiebreak
} Query;

// ==================== VARIABLES GLOBALES ====================

Cliente clientes[MAX_CLIENTES];
Query queries[MAX_QUERIES];

int cantidad_workers = 0;
int cantidad_queries = 0;
int next_query_id = 0;
int next_worker_id = 0;

pthread_mutex_t mutex_planificador = PTHREAD_MUTEX_INITIALIZER;

// CONFIG desde master_configs (suponemos que ya existe)
extern MasterConfig master_configs; // contiene puertoescucha, algoritmo, tiempo_aging, loglevel

// ==================== UTILIDADES ====================

static long long ahora_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

int encontrar_slot_cliente() {
    for (int i = 0; i < MAX_CLIENTES; i++)
        if (!clientes[i].activo) return i;
    return -1;
}

Cliente* buscar_worker_libre() {
    for (int i = 0; i < MAX_CLIENTES; i++) {
        if (clientes[i].activo && clientes[i].tipo == CLIENTE_WORKER && !clientes[i].ocupado)
            return &clientes[i];
    }
    return NULL;
}

Query* buscar_exec_de_menor_prioridad() {
    Query* candidato = NULL;
    for (int i = 0; i < next_query_id; i++) {
        if (queries[i].id == i && queries[i].estado == EXEC) {
            if (candidato == NULL) candidato = &queries[i];
            else if (queries[i].prioridad > candidato->prioridad ||
                    (queries[i].prioridad == candidato->prioridad &&
                     queries[i].timestamp_entrada < candidato->timestamp_entrada)) {
                candidato = &queries[i];
            }
        }
    }
    return candidato;
}

Query* buscar_query_por_socket_worker(int sock) {
    for (int i = 0; i < next_query_id; i++)
        if (queries[i].id == i && queries[i].socket_worker == sock)
            return &queries[i];
    return NULL;
}

Query* buscar_query_por_id(int id) {
    if (id < 0 || id >= next_query_id) return NULL;
    if (queries[id].id != id) return NULL;
    return &queries[id];
}

Cliente* buscar_cliente_por_socket(int sock) {
    for (int i = 0; i < MAX_CLIENTES; i++)
        if (clientes[i].activo && clientes[i].socket == sock)
            return &clientes[i];
    return NULL;
}

// ==================== REGISTRO / ELIMINACIÓN ====================

void registrar_worker(int socket) {
    int slot = encontrar_slot_cliente();
    if (slot == -1) {
        printf("No hay slots para más clientes\n");
        close(socket);
        return;
    }

    clientes[slot].activo = true;
    clientes[slot].socket = socket;
    clientes[slot].tipo = CLIENTE_WORKER;
    clientes[slot].id = next_worker_id++;
    clientes[slot].ocupado = false;
    cantidad_workers++;

    // --- LOG: Registrar Worker ---
    log_info(logger_master, "## Se conecta el Worker %d - Total Workers: %d",
             clientes[slot].id, cantidad_workers);
}

void registrar_querycontrol(int socket) {
    char* mensaje = recibir_mensaje(socket);
    if (!mensaje) {
        printf("Error al recibir mensaje del Query Control\n");
        return;
    }

    char** partes = string_split(mensaje, "|");
    char* path_query = partes[0] ? strdup(partes[0]) : strdup("UNKNOWN");
    int prioridad = (partes[1] != NULL) ? atoi(partes[1]) : 5;

    int qid = next_query_id++;
    Query* q = &queries[qid];
    q->id = qid;
    q->socket_query = socket;
    q->socket_worker = -1;
    strncpy(q->archivo_query, path_query, sizeof(q->archivo_query) - 1);
    q->archivo_query[sizeof(q->archivo_query) - 1] = '\0';
    q->prioridad = prioridad;
    q->estado = READY;
    q->program_counter = 0;
    q->timestamp_entrada = ahora_ms();
    cantidad_queries++;

    // --- LOG: Registrar QueryControl ---
    log_info(logger_master,
        "## Nuevo QueryControl: Query %s, prioridad %d, id %d (Workers: %d)",
        q->archivo_query, q->prioridad, q->id, cantidad_workers);

    free(path_query);
    free(mensaje);
    string_array_destroy(partes);
}

void eliminar_cliente_y_cancelar_query(int socket) {
    Cliente* c = buscar_cliente_por_socket(socket);
    if (!c) {
        close(socket);
        return;
    }

    if (c->tipo == CLIENTE_WORKER) {
        Query* q = buscar_query_por_socket_worker(socket);
        if (q) {
            q->estado = EXIT;
            enviar_mensaje("END|Error: Worker desconectado", q->socket_query);
            // --- LOG: Desconexión de Worker con query terminada ---
            log_info(logger_master,
                     "## Worker %d desconectado - Query %d finalizada",
                     c->id, q->id);
        } else {
            // --- LOG: Desconexión de Worker sin query ---
            log_info(logger_master,
                     "## Worker %d desconectado (sin query asignada)", c->id);
        }
        cantidad_workers--;
    } else {
        for (int i = 0; i < next_query_id; i++) {
            if (queries[i].id == i && queries[i].socket_query == socket) {
                if (queries[i].estado == READY) {
                    queries[i].estado = EXIT;
                    // --- LOG: Desconexión de QueryControl con query cancelada ---
                    log_info(logger_master,
                        "## QueryControl desconectado - Query %d (READY) cancelada",
                        queries[i].id);
                } else if (queries[i].estado == EXEC) {
                    int sock_worker = queries[i].socket_worker;
                    if (sock_worker != -1) enviar_mensaje("PREEMPT", sock_worker);
                    // --- LOG: Desconexión de QueryControl por desalojo de query ---
                    log_info(logger_master,
                        "## QueryControl desconectado - Desalojo de Query %d solicitado",
                        queries[i].id);
                }
            }
        }
    }

    close(c->socket);
    c->activo = false;
    c->ocupado = false;
}

// ==================== PLANIFICADOR ====================

void enviar_query_a_worker(Query* q, Cliente* w) {
    if (!q || !w) return;
    w->ocupado = true;
    q->socket_worker = w->socket;
    q->estado = EXEC;
    q->timestamp_entrada = ahora_ms();

    // --- LOG: Envío de query ---
    log_info(logger_master,
        "## Envío de Query %d (prioridad %d) al Worker %d",
        q->id, q->prioridad, w->id);

    enviar_mensaje(q->archivo_query, w->socket);

    if (q->program_counter != 0) {
        char buf[128];
        snprintf(buf, sizeof(buf), "RESUME|%d", q->program_counter);
        enviar_mensaje(buf, w->socket);
    }
}

Query* seleccionar_mejor_ready() {
    Query* mejor = NULL;
    long long mejor_ts = 0;
    for (int i = 0; i < next_query_id; i++) {
        if (queries[i].id == i && queries[i].estado == READY) {
            if (!mejor ||
                queries[i].prioridad < mejor->prioridad ||
                (queries[i].prioridad == mejor->prioridad &&
                 queries[i].timestamp_entrada < mejor_ts)) {
                mejor = &queries[i];
                mejor_ts = queries[i].timestamp_entrada;
            }
        }
    }
    return mejor;
}

void* planificador_prioridades(void* arg) {
    while (1) {
        pthread_mutex_lock(&mutex_planificador);

        Cliente* w = buscar_worker_libre();
        Query* mejor = seleccionar_mejor_ready();
        if (mejor && w) {
            enviar_query_a_worker(mejor, w);
            pthread_mutex_unlock(&mutex_planificador);
            usleep(100 * 1000);
            continue;
        }

        if (!w) {
            Query* ready_mejor = seleccionar_mejor_ready();
            Query* exec_peor = buscar_exec_de_menor_prioridad();
            if (ready_mejor && exec_peor && ready_mejor->prioridad < exec_peor->prioridad) {
                int sock_worker = exec_peor->socket_worker;
                Cliente* cw = buscar_cliente_por_socket(sock_worker);
                if (cw) {
                    // --- LOG: Desalojo de query/worker por prioridad ---
                    log_info(logger_master,
                        "## Desalojo: Query %d (%d) del Worker %d por prioridad",
                        exec_peor->id, exec_peor->prioridad, cw->id);
                    enviar_mensaje("PREEMPT", sock_worker);
                }
            }
        }

        pthread_mutex_unlock(&mutex_planificador);
        usleep(200 * 1000);
    }
    return NULL;
}

// ==================== AGING ====================

void* hilo_aging(void* arg) {
    int tiempo_aging = master_configs.tiempo_aging;
    if (tiempo_aging <= 0) return NULL;

    while (1) {
        usleep(tiempo_aging * 1000);
        pthread_mutex_lock(&mutex_planificador);

        for (int i = 0; i < next_query_id; i++) {
            if (queries[i].id == i && queries[i].estado == READY && queries[i].prioridad > 0) {
                int antes = queries[i].prioridad;
                queries[i].prioridad--;
                // --- LOG: Cambio de prioridad por aging ---
                log_info(logger_master,
                    "## AGING: Query %d cambia prioridad %d -> %d",
                    queries[i].id, antes, queries[i].prioridad);
            }
        }

        pthread_mutex_unlock(&mutex_planificador);
    }
    return NULL;
}

// ==================== MAIN ====================

int main(int argc, char* argv[]) {
    saludar("master");
    inicializar_configs();
    inicializar_logger_master(master_configs.loglevel);
    // --- LOG: Master iniciado ---
    log_info(logger_master, "## Master inicializado (nivel log %d)", master_configs.loglevel);

    int server_fd = iniciar_servidor(string_itoa(master_configs.puertoescucha));
    if (server_fd < 0) {
        // --- LOG: Error al iniciar Master ---
        log_error(logger_master, "Error al iniciar servidor en puerto %d", master_configs.puertoescucha);
        exit(EXIT_FAILURE);
    }

    // --- LOG: Master escuchando... ---
    log_info(logger_master, "## Master escuchando en puerto %d", master_configs.puertoescucha);

    for (int i = 0; i < MAX_CLIENTES; i++) clientes[i].activo = false;
    for (int i = 0; i < MAX_QUERIES; i++) queries[i].id = -1;

    fd_set master_set, read_fds;
    int fdmax = server_fd;
    FD_ZERO(&master_set);
    FD_SET(server_fd, &master_set);

    pthread_t th_plan, th_aging;
    pthread_create(&th_plan, NULL, planificador_prioridades, NULL);
    if (master_configs.tiempo_aging > 0)
        pthread_create(&th_aging, NULL, hilo_aging, NULL);

    while (1) {
        read_fds = master_set;
        if (select(fdmax + 1, &read_fds, NULL, NULL, NULL) == -1) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }

        for (int i = 0; i <= fdmax; i++) {
            if (!FD_ISSET(i, &read_fds)) continue;

            if (i == server_fd) {
                int nuevo_socket = esperar_cliente(server_fd);
                if (nuevo_socket <= 0) continue;

                FD_SET(nuevo_socket, &master_set);
                if (nuevo_socket > fdmax) fdmax = nuevo_socket;

                char* tipo = recibir_mensaje(nuevo_socket);
                if (!tipo) {
                    close(nuevo_socket);
                    FD_CLR(nuevo_socket, &master_set);
                    continue;
                }

                if (strcmp(tipo, "WORKER") == 0)
                    registrar_worker(nuevo_socket);
                else
                    registrar_querycontrol(nuevo_socket);

                free(tipo);
            } else {
                char* mensaje = recibir_mensaje(i);
                if (!mensaje) {
                    pthread_mutex_lock(&mutex_planificador);
                    eliminar_cliente_y_cancelar_query(i);
                    pthread_mutex_unlock(&mutex_planificador);
                    FD_CLR(i, &master_set);
                    continue;
                }

                Cliente* c = buscar_cliente_por_socket(i);
                if (!c) {
                    free(mensaje);
                    close(i);
                    FD_CLR(i, &master_set);
                    continue;
                }

                if (c->tipo == CLIENTE_WORKER) {
                    if (strncmp(mensaje, "READ|", 5) == 0) {
                        pthread_mutex_lock(&mutex_planificador);
                        Query* q = buscar_query_por_socket_worker(i);
                        if (q)
                            enviar_mensaje(mensaje, q->socket_query);
                        pthread_mutex_unlock(&mutex_planificador);
                    } else if (strncmp(mensaje, "END|", 4) == 0) {
                        pthread_mutex_lock(&mutex_planificador);
                        Query* q = buscar_query_por_socket_worker(i);
                        if (q) {
                            enviar_mensaje(mensaje, q->socket_query);
                            q->estado = EXIT;
                            q->socket_worker = -1;
                            c->ocupado = false;
                        }
                        pthread_mutex_unlock(&mutex_planificador);
                    } else if (strncmp(mensaje, "PC|", 3) == 0) {
                        int pc = atoi(mensaje + 3);
                        pthread_mutex_lock(&mutex_planificador);
                        Query* q = buscar_query_por_socket_worker(i);
                        if (q) {
                            q->program_counter = pc;
                            q->socket_worker = -1;
                            q->estado = READY;
                            q->timestamp_entrada = ahora_ms();
                            c->ocupado = false;
                            // --- LOG: Desalojo de query/worker por prioridad ---
                            log_info(logger_master,
                                "## Se desaloja Query %d (%d) del Worker %d - PRIORIDAD (PC=%d)",
                                q->id, q->prioridad, c->id, pc);
                        } else {
                            // --- LOG: PC sin query asociada ---
                            log_warning(logger_master,
                                "## PC recibido sin Query asociada al Worker %d", c->id);
                        }
                        pthread_mutex_unlock(&mutex_planificador);
                    } else {
                        // --- LOG: Mensaje no esperado ---
                        log_debug(logger_master,
                            "## Mensaje desconocido desde Worker %d: %s", c->id, mensaje);
                    }
                } else { // Query Control
                    if (strncmp(mensaje, "CANCEL|", 7) == 0) {
                        int qid = atoi(mensaje + 7);
                        pthread_mutex_lock(&mutex_planificador);
                        Query* q = buscar_query_por_id(qid);
                        if (q) {
                            if (q->estado == READY) {
                                q->estado = EXIT;
                                // --- LOG: Query cancelada ---
                                log_info(logger_master, "## Query %d cancelada (READY)", qid);
                            } else if (q->estado == EXEC && q->socket_worker != -1) {
                                enviar_mensaje("PREEMPT", q->socket_worker);
                                // --- LOG: Cancelar Query ---
                                log_info(logger_master,
                                    "## Se solicita cancelación de Query %d en ejecución", qid);
                            }
                        }
                        pthread_mutex_unlock(&mutex_planificador);
                    } else {
                        // --- LOG: Mensaje del QueryControl ---
                        log_debug(logger_master,
                            "## Mensaje desde Query Control socket %d: %s", i, mensaje);
                    }
                }

                free(mensaje);
            }
        }
    }

    close(server_fd);
    return 0;
}
