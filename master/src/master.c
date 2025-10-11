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

#define MAX_WORKERS 200
#define MAX_QUERY_CONTROLS 200
#define MAX_QUERIES  200

typedef enum { READY, EXEC, EXIT } EstadoQuery;

typedef struct {
    int socket;
    int id;
    bool activo;
    bool ocupado;
} Worker;

typedef struct {
    int socket;
    bool activo;
} QueryControl;

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

Worker workers[MAX_WORKERS];
QueryControl query_controls[MAX_QUERY_CONTROLS];
Query queries[MAX_QUERIES];

int cantidad_workers = 0;
int next_worker_id = 0;

int cantidad_query_controls = 0;

int cantidad_queries = 0;
int next_query_id = 0;

pthread_mutex_t mutex_planificador = PTHREAD_MUTEX_INITIALIZER;

// CONFIG desde master_configs (suponemos que ya existe)
extern MasterConfig master_configs; // contiene puertoescucha, algoritmo, tiempo_aging, loglevel

// ==================== UTILIDADES ====================

static long long ahora_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec * 1000LL + ts.tv_nsec / 1000000LL;
}

int encontrar_slot_worker() {
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (!workers[i].activo)
            return i;
    }
    return -1;
}

int encontrar_slot_query_control() {
    for (int i = 0; i < MAX_QUERY_CONTROLS; i++) {
        if (!query_controls[i].activo)
            return i;
    }
    return -1;
}

Worker* buscar_worker_libre() {
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (workers[i].activo && !workers[i].ocupado)
            return &workers[i];
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

Worker* buscar_worker_por_socket(int socket) {
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (workers[i].activo && workers[i].socket == socket)
            return &workers[i];
    }
    return NULL;
}

QueryControl* buscar_query_control_por_socket(int socket) {
    for (int i = 0; i < MAX_QUERY_CONTROLS; i++) {
        if (query_controls[i].activo && query_controls[i].socket == socket)
            return &query_controls[i];
    }
    return NULL;
}

// ==================== REGISTRO / ELIMINACIÓN ====================

void registrar_worker(int socket) {
    int slot = -1;
    for (int i = 0; i < MAX_WORKERS; i++) {
        if (!workers[i].activo) {
            slot = i;
            break;
        }
    }
    if (slot == -1) {
        printf("No hay espacio para más workers\n");
        close(socket);
        return;
    }

    workers[slot].socket = socket;
    workers[slot].activo = true;
    workers[slot].ocupado = false;
    workers[slot].id = next_worker_id++;

    cantidad_workers++;
    log_info(logger_master, "## Se conecta el Worker %d - Total Workers: %d",
             workers[slot].id, cantidad_workers);
}

void registrar_query_control(int socket) {
    int slot = -1;
    for (int i = 0; i < MAX_QUERY_CONTROLS; i++) {
        if (!query_controls[i].activo) {
            slot = i;
            break;
        }
    }
    if (slot == -1) {
        printf("No hay espacio para más Query Controls\n");
        close(socket);
        return;
    }

    query_controls[slot].socket = socket;
    query_controls[slot].activo = true;
    cantidad_query_controls++;

    log_info(logger_master, "## Se conecta un Query Control - Total: %d", cantidad_query_controls);
}

void eliminar_worker_y_cancelar_query(int socket) {
    Worker* w = buscar_worker_por_socket(socket);
    if (!w) {
        close(socket);
        return;
    }

    Query* q = buscar_query_por_socket_worker(socket);
    if (q) {
        q->estado = EXIT;
        enviar_mensaje("END|Error: Worker desconectado", q->socket_query);

        // --- LOG ---
        log_info(logger_master,
                 "## Worker %d desconectado - Query %d finalizada",
                 w->id, q->id);
    } else {
        // --- LOG ---
        log_info(logger_master,
                 "## Worker %d desconectado (sin query asignada)", w->id);
    }

    close(w->socket);
    w->activo = false;
    w->ocupado = false;
    cantidad_workers--;
}

void eliminar_querycontrol_y_cancelar_query(int socket) {
    QueryControl* qc = buscar_query_control_por_socket(socket);
    if (!qc) {
        close(socket);
        return;
    }

    for (int i = 0; i < next_query_id; i++) {
        if (queries[i].id == i && queries[i].socket_query == socket) {
            if (queries[i].estado == READY) {
                queries[i].estado = EXIT;
                log_info(logger_master,
                         "## QueryControl desconectado - Query %d (READY) cancelada",
                         queries[i].id);
            } else if (queries[i].estado == EXEC) {
                int sock_worker = queries[i].socket_worker;
                if (sock_worker != -1)
                    enviar_mensaje("PREEMPT", sock_worker);

                log_info(logger_master,
                         "## QueryControl desconectado - Desalojo de Query %d solicitado",
                         queries[i].id);
            }
        }
    }

    close(qc->socket);
    qc->activo = false;
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

        Worker* w = buscar_worker_libre();
        Query* mejor = seleccionar_mejor_ready();

        // --- Caso 1: hay query lista y worker libre ---
        if (mejor && w) {
            enviar_query_a_worker(mejor, w);
            pthread_mutex_unlock(&mutex_planificador);
            usleep(100 * 1000);
            continue;
        }

        // --- Caso 2: no hay worker libre → posible desalojo por prioridad ---
        if (!w) {
            Query* ready_mejor = seleccionar_mejor_ready();
            Query* exec_peor = buscar_exec_de_menor_prioridad();

            if (ready_mejor && exec_peor && ready_mejor->prioridad < exec_peor->prioridad) {
                int sock_worker = exec_peor->socket_worker;
                Worker* cw = buscar_worker_por_socket(sock_worker);
                if (cw) {
                    // --- LOG ---
                    log_info(logger_master,
                        "## Desalojo: Query %d (prioridad %d) del Worker %d por prioridad superior (%d)",
                        exec_peor->id, exec_peor->prioridad, cw->id, ready_mejor->prioridad);

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

    //Valido que se haya ejecutado correctamente
    if (argc != 2) {
        fprintf(stderr, "Uso: %s [archivo_config] \n", argv[0]);
        return EXIT_FAILURE;
    }

    //Cargo el archivo config en una variable
    char* path_config = argv[1];

    //Inicializo las configs de Master
    inicializar_configs(path_config);

    //Inicializo el logger
    inicializar_logger_master(master_configs.loglevel);

    // --- LOG: Master iniciado ---
    log_info(logger_master, "## Master inicializado (nivel log %d)", master_configs.loglevel);

    
    // Iniciar servidor: preparar puerto como cadena si iniciar_servidor lo necesita
    char* puerto_str = string_itoa(master_configs.puertoescucha);
    int server_fd = iniciar_servidor(puerto_str);
    free(puerto_str);
    
    if (server_fd < 0) {
        // --- LOG: Error al iniciar Master ---
        log_error(logger_master, "Error al iniciar servidor en puerto %d", master_configs.puertoescucha);
        exit(EXIT_FAILURE);
    }

    // --- LOG: Master escuchando... ---
    log_info(logger_master, "## Master escuchando en puerto %d", master_configs.puertoescucha);

    // Inicializo listas de Workers y QueryControls
    for (int i = 0; i < MAX_WORKERS; i++) {
        workers[i].activo = false;
        workers[i].socket = -1;
        workers[i].ocupado = false;
        workers[i].id = -1;
    }

    for (int i = 0; i < MAX_QUERY_CONTROLS; i++) {
        querycontrols[i].activo = false;
        querycontrols[i].socket = -1;
        querycontrols[i].id = -1;
    }
    for (int i = 0; i < MAX_QUERIES; i++) {
        queries[i].id = -1;
        queries[i].socket_query = -1;
        queries[i].socket_worker = -1;
    }

    // Lanzar hilos: planificador y aging
    pthread_t th_planificador, th_aging;
    if (pthread_create(&th_planificador, NULL, planificador_prioridades, NULL) != 0) {
        log_error(logger_master, "Error al crear hilo planificador: %s", strerror(errno));
        close(server_fd);
        return EXIT_FAILURE;
    }
    // sólo lanzar aging si corresponde
    if (master_configs.tiempo_aging > 0) {
        if (pthread_create(&th_aging, NULL, hilo_aging, NULL) != 0) {
            log_error(logger_master, "Error al crear hilo de aging: %s", strerror(errno));
            // no es fatal: continuar sin aging o decide salir
        }
    }
    
    // Preparar select()
    fd_set master_set, read_fds;
    int fdmax = server_fd;
    FD_ZERO(&master_set);
    FD_SET(server_fd, &master_set);

    // Bucle principal: accept + mensajes
    while (1) {
        read_fds = master_set;
        int ready_count = select(fdmax + 1, &read_fds, NULL, NULL, NULL);
        if (ready_count == -1) {
            if (errno == EINTR) continue;
            log_error(logger_master, "select() falló: %s", strerror(errno));
            break;
        }

        for (int fd = 0; fd <= fdmax && ready_count > 0; ++fd) {
            if (!FD_ISSET(fd, &read_fds)) continue;
            ready_count--;

            // ==== NUEVA CONEXIÓN ====
            if (fd == server_fd) {
                int nuevo_socket = esperar_cliente(server_fd);
                if (nuevo_socket <= 0) continue;


                // Agregar a master_set y actualizar fdmax
                FD_SET(nuevo_socket, &master_set);
                if (nuevo_socket > fdmax) fdmax = nuevo_socket;

                // Recibir handshake inicial: tipo de cliente
                t_paquete* handshake = recibir_paquete(nuevo_socket);
                if (!handshake) {
                    close(nuevo_socket);
                    FD_CLR(nuevo_socket, &master_set);
                    continue;
                }

                if (handshake->codigo_operacion == HANDSHAKE_WORKER) {
                    registrar_worker(nuevo_socket);
                    log_info(logger_master, "Se conectó un Worker (socket %d)", nuevo_socket);
                }
                else if (handshake->codigo_operacion == HANDSHAKE_QUERYCONTROL) {
                    registrar_querycontrol(nuevo_socket);
                    log_info(logger_master, "Se conectó un QueryControl (socket %d)", nuevo_socket);
                }
                else {
                    log_warning(logger_master, "Handshake desconocido en socket %d", nuevo_socket);
                    close(nuevo_socket);
                    FD_CLR(nuevo_socket, &master_set);
                }

                destruir_paquete(handshake);
                continue;
            }
            
            // ==== CLIENTE YA CONECTADO ====
            t_paquete* paquete = recibir_paquete(fd);
            if (!paquete) {
                // Cliente desconectadof (buscar_worker_por_socket(fd))
                    eliminar_worker_y_cancelar_query(fd);
                else if (buscar_querycontrol_por_socket(fd))
                    eliminar_querycontrol_y_cancelar_query(fd);
                else
                    log_warning(logger_master, "## Socket %d desconectado, pero no pertenece a ningún cliente", fd);

                pthread_mutex_unlock(&mutex_planificador);

                FD_CLR(fd, &master_set);
                close(fd);
                continue;
            }

            // ==== Identificar tipo de cliente ====
            Worker* worker = buscar_worker_por_socket(fd);
            QueryControl* qc = buscar_querycontrol_por_socket(fd);
            
            if (worker != NULL) {
            // ======= MENSAJE DESDE WORKER =======
            switch (paquete->codigo_operacion) {
                case READ: {
                    t_operacion_query* op = deserializar_operacion_query(paquete->buffer);
                    Query* q = buscar_query_por_socket_worker(fd);
                    if (q) enviar_operacion_query(op, q->socket_query);
                    destruir_operacion_query(op);
                    break;
                }

                case END: {
                    char* motivo = deserializar_string(paquete->buffer);
                    Query* q = buscar_query_por_socket_worker(fd);
                    if (q) {
                        enviar_mensaje_formato(q->socket_query, "END|%s", motivo);
                        q->estado = EXIT;
                        worker->ocupado = false;
                        log_info(logger_master, "## Worker %d finalizó Query %d (%s)", worker->id, q->id, motivo);
                    }
                    free(motivo);
                    break;
                }

                case PROGRAM_COUNTER: {
                    int pc = deserializar_int(paquete->buffer);
                    Query* q = buscar_query_por_socket_worker(fd);
                    if (q) {
                        q->program_counter = pc;
                        q->estado = READY;
                        q->timestamp_entrada = ahora_ms();
                        worker->ocupado = false;
                        log_info(logger_master, "## Desalojo: Query %d (PC=%d) vuelve a READY", q->id, pc);
                    }
                    break;
                }

                default:
                    log_warning(logger_master, "## Código desconocido desde Worker %d: %d", worker->id, paquete->codigo_operacion);
                    break;
            }
        } 
            
                else if (qc != NULL) {
            // ======= MENSAJE DESDE QUERYCONTROL =======
            switch (paquete->codigo_operacion) {
                case NEW_QUERY: {
                    Query* nueva = deserializar_query(paquete->buffer);
                    registrar_query(nueva, qc->socket);
                    log_info(logger_master, "## Nueva Query %d recibida (prioridad %d)", nueva->id, nueva->prioridad);
                    break;
                }

                case CANCEL_QUERY: {
                    int qid = deserializar_int(paquete->buffer);
                    pthread_mutex_lock(&mutex_planificador);
                    Query* q = buscar_query_por_id(qid);
                    if (q) {
                        if (q->estado == READY) {
                            q->estado = EXIT;
                            log_info(logger_master, "## Query %d cancelada (READY)", qid);
                        } else if (q->estado == EXEC && q->socket_worker != -1) {
                            enviar_mensaje("PREEMPT", q->socket_worker);
                            log_info(logger_master, "## Cancelación solicitada de Query %d (EXEC)", qid);
                        }
                    }
                    pthread_mutex_unlock(&mutex_planificador);
                    break;
                }

                default:
                    log_warning(logger_master, "## Código desconocido desde QueryControl: %d", paquete->codigo_operacion);
                    break;
            }
        } 
        else {
            log_warning(logger_master, "Socket %d no pertenece a ningún cliente registrado", fd);
            close(fd);
            FD_CLR(fd, &master_set);
        }

        destruir_paquete(paquete);
    } 
}  

    close(server_fd);
    log_info(logger_master, "Master finalizando.");
    return 0;
}

void* servidor_general(){
    //Inicio el servidor
    int servidor = iniciar_servidor(string_itoa(master_configs.puertoescucha));

    while(1) {

        //Espero una conexión
        int cliente = esperar_cliente(servidor);

        //Recibo un paquete del cliente
        t_paquete* paquete = recibir_paquete(cliente);

        //Creo una estructura para pasarle a los hilos
        t_conexion* conexion = malloc(sizeof(t_conexion));
        conexion->paquete = paquete;
        conexion->cliente = cliente;

        switch(paquete->codigo_operacion){
            
            //Si se conecta un Query Control
            case PAQUETE_QUERY:

                //Creo un hilo para manejarla
                pthread_t hilo_querycontrol;
                pthread_create(&hilo_querycontrol, NULL, (void*) atender_query_control, (void*) conexion);
                pthread_detach(hilo_querycontrol);
                break;
            
            //Si se conecta un Worker
            case PAQUETE_WORKER:

                //Creo un hilo para manejarlo
                pthread_t hilo_worker;
                pthread_create(&hilo_worker, NULL, (void*) atender_worker, (void*) conexion);
                pthread_detach(hilo_worker);
                break;
        }  
    }
 
    return NULL;
}

