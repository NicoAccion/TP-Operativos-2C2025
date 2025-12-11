#include "worker_memoria.h"
#include "worker-configs.h"
#include "worker-log.h"
#include "worker.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <limits.h>

// --- Variables estáticas del módulo de memoria ---
static void* memoria_principal;
static PaginaMemoria* tabla_de_marcos;
static int cantidad_marcos;
static int tam_pagina;
static int puntero_clock = 0;

void inicializar_memoria(int tam_mem, int tam_pag) {
    memoria_principal = calloc(1, tam_mem);
    if (memoria_principal == NULL) {
        log_error(logger_worker, "Error fatal: malloc falló para memoria_principal");
        exit(EXIT_FAILURE);
    }
    cantidad_marcos = tam_mem / tam_pag;
    tam_pagina = tam_pag;
    tabla_de_marcos = calloc(cantidad_marcos, sizeof(PaginaMemoria));
    if (tabla_de_marcos == NULL) {
        log_error(logger_worker, "Error fatal: calloc falló para tabla_de_marcos");
        free(memoria_principal);
        exit(EXIT_FAILURE);
    }

    log_info(logger_worker, "Memoria inicializada: %d bytes (%d marcos de %d bytes)", tam_mem, cantidad_marcos, tam_pag);
}

void liberar_memoria() {
    free(memoria_principal);
    free(tabla_de_marcos);
}

static int obtener_marco_libre() {
    for (int i = 0; i < cantidad_marcos; i++) {
        if (!tabla_de_marcos[i].ocupado) {
            return i;
        }
    }
    return -1; // No hay marcos libres
}

static int reemplazar_pagina(int query_id, int socket_storage,
                             int socket_master,
                             const char* nuevo_file,
                             const char* nuevo_tag,
                             int nuevo_num_pagina) {
    int marco_victima = -1;
    
    if (strcasecmp(worker_configs.algoritmoreemplazo, "LRU") == 0) {
        // LRU
        unsigned long long mas_antiguo = ULLONG_MAX;
        
        for (int i = 0; i < cantidad_marcos; i++) {
            if (tabla_de_marcos[i].ocupado && tabla_de_marcos[i].timestamp < mas_antiguo) {
                mas_antiguo = tabla_de_marcos[i].timestamp;
                marco_victima = i;
            }
        }

        log_info(logger_worker, "## LRU Marco víctima: %d (timestamp más antiguo: %llu)", 
                 marco_victima, mas_antiguo);

    }

    else if (strcasecmp(worker_configs.algoritmoreemplazo, "CLOCK-M") == 0) {
        // CLOCK-M: Algoritmo de 4 pasos 
        // Paso 1: Buscar (U=0, M=0). No tocar nada.
        // Paso 2: Buscar (U=0, M=1). Poner U=0 a los que pasemos.
        
        int start_pointer = puntero_clock;
        int pasos = 0;

        // Maximo 2 iteraciones completas sobre la memoria para cubrir los 4 pasos logicos
        while (marco_victima == -1 && pasos < 4) { 
            
            for (int i = 0; i < cantidad_marcos; i++) {
                int idx = (start_pointer + i) % cantidad_marcos;
                PaginaMemoria* pag = &tabla_de_marcos[idx];

                if (!pag->ocupado) { // Si esta libre, uso directo
                    marco_victima = idx;
                    break;
                }

                // Pasos 1 y 3: Buscamos U=0, M=0
                if (pasos == 0 || pasos == 2) {
                    if (!pag->usado && !pag->modificado) {
                        marco_victima = idx;
                        break;
                    }
                }
                
                // Pasos 2 y 4: Buscamos U=0, M=1. bajamos el bit de uso (U=0)
                if (pasos == 1 || pasos == 3) {
                    if (!pag->usado && pag->modificado) {
                        marco_victima = idx;
                        break;
                    }
                    // Efecto colateral del paso 2 (y 4): bajar bandera de uso
                    pag->usado = false; 
                }
            }
            
            if (marco_victima != -1) {
                puntero_clock = (marco_victima + 1) % cantidad_marcos; // Avanzar puntero para la proxima
                break;
            }
            pasos++;
        }
        log_info(logger_worker, "## CLOCK-M Marco víctima: %d", marco_victima);
    }
    else {
        log_warning(logger_worker, "Algoritmo de reemplazo desconocido. Usando LRU por defecto.");
        marco_victima = 0;
    }
    
    
    PaginaMemoria* victima = &tabla_de_marcos[marco_victima];

    log_info(logger_worker, "## Query %d: Se reemplaza la página %s:%s/%d por la %s:%s/%d", 
             query_id, victima->file, victima->tag, victima->num_pagina, nuevo_file, nuevo_tag, nuevo_num_pagina);

    if (victima->modificado) {
        log_info(logger_worker, "La página víctima (Marco %d) está modificada. FLUSH a Storage.", marco_victima);
        
        t_op_storage* op_flush = calloc(1, sizeof(t_op_storage));
        op_flush->query_id = query_id;
        op_flush->nombre_file = strdup(victima->file);
        op_flush->nombre_tag = strdup(victima->tag);
        op_flush->direccion_base = victima->num_pagina;
        
        // cambio bin
        op_flush->tamano_contenido = tam_pagina; 
        op_flush->contenido = malloc(tam_pagina);
        memcpy(op_flush->contenido, memoria_principal + (marco_victima * tam_pagina), tam_pagina);

        enviar_op_simple_storage(socket_storage, socket_master, WRITE, op_flush);
    }

    // Liberar marco para su reutilización
    victima->ocupado = false;
    victima->modificado = false;
    victima->usado = false;
    victima->timestamp = 0;
    victima->file[0] = '\0';
    victima->tag[0] = '\0';
    victima->num_pagina = -1;

    log_info(logger_worker, "Query %d: Se libera el Marco: %d perteneciente al - File: %s - Tag: %s",
             query_id, marco_victima, victima->file, victima->tag);

    return marco_victima;
}

// Busca si una página ya está en memoria
static int buscar_pagina_en_memoria(const char* file, const char* tag, int num_pagina) {
    for (int i = 0; i < cantidad_marcos; i++) {
        if (tabla_de_marcos[i].ocupado &&
            tabla_de_marcos[i].num_pagina == num_pagina &&
            strcmp(tabla_de_marcos[i].file, file) == 0 &&
            strcmp(tabla_de_marcos[i].tag, tag) == 0) 
        {
            return i; // Page Hit
        }
    }
    return -1; // Page Fault
}

void escribir_en_memoria(int query_id, const char* file, const char* tag, int direccion_logica, const char* contenido, int socket_storage, int socket_master) {
    
    int bytes_totales = strlen(contenido) + 1; // +1 para incluir el \0
    int bytes_escritos = 0;
    int dir_logica_actual = direccion_logica;

    // BUCLE PARA ESCRIBIR EN MÚLTIPLES PÁGINAS SI ES NECESARIO
    while (bytes_escritos < bytes_totales) {
        
        int num_pagina = dir_logica_actual / tam_pagina;
        int offset = dir_logica_actual % tam_pagina;
        
        // 1. Buscar si la página ya está en memoria (o traerla)
        int marco = buscar_pagina_en_memoria(file, tag, num_pagina);
        
        if (marco == -1) {
            // --- PAGE FAULT ---
            log_info(logger_worker, "## Query %d: (WRITE) Memoria Miss - File: %s - Tag: %s Pagina: %d", 
                    query_id, file, tag, num_pagina);
            
            marco = obtener_marco_libre();
            if (marco == -1) {
                marco = reemplazar_pagina(query_id, socket_storage, socket_master, file, tag, num_pagina);
            }
            
            // Traer contenido original de Storage (para no romper el resto de la página)
            t_op_storage* op_read_req = calloc(1, sizeof(t_op_storage));
            op_read_req->query_id = query_id;
            op_read_req->nombre_file = strdup(file);
            op_read_req->nombre_tag  = strdup(tag);
            op_read_req->direccion_base = num_pagina;
            op_read_req->tamano = tam_pagina; 

            char* contenido_bloque = enviar_op_read_storage(socket_storage, socket_master, op_read_req);

            if (contenido_bloque != NULL) {
                memcpy(memoria_principal + (marco * tam_pagina), contenido_bloque, tam_pagina);
                free(contenido_bloque);
            } else {
                memset(memoria_principal + (marco * tam_pagina), 0, tam_pagina);
            }
             log_info(logger_worker, "## Query %d: Memoria Add - Pagina: %d Marco: %d", query_id, num_pagina, marco);
        } else {
             log_info(logger_worker, "## Query %d: (WRITE) Memoria Hit - Pagina: %d Marco: %d", query_id, num_pagina, marco);
        }    

        // 2. Retardo (Simulamos acceso a memoria por cada página tocada)
        usleep(worker_configs.retardomemoria * 1000);

        // 3. Calcular cuánto escribimos EN ESTA página
        int bytes_disponibles = tam_pagina - offset;
        int bytes_restantes = bytes_totales - bytes_escritos;
        
        // Escribimos lo que quede o lo que entre, lo que sea menor
        int bytes_a_escribir_ahora = (bytes_restantes < bytes_disponibles) ? bytes_restantes : bytes_disponibles;

        // 4. Escribir en RAM
        int direccion_fisica = (marco * tam_pagina) + offset;
        void* contenido_actual_ptr = (void*)(contenido + bytes_escritos);
        
        memcpy(memoria_principal + direccion_fisica, contenido_actual_ptr, bytes_a_escribir_ahora);

        // Solo logueamos el contenido parcial para no ensuciar tanto, o el total si entra
        // log_info(logger_worker, "## Query %d: Escribiendo en DirFisica: %d (%d bytes)", query_id, direccion_fisica, bytes_a_escribir_ahora);

        // 5. Actualizar Metadata de la Página
        PaginaMemoria* pagina_info = &tabla_de_marcos[marco];
        pagina_info->ocupado = true;
        pagina_info->modificado = true; 
        pagina_info->usado = true;
        pagina_info->timestamp = (unsigned long long)time(NULL); 
        pagina_info->num_pagina = num_pagina;
        strncpy(pagina_info->file, file, MAX_FILETAG - 1);
        strncpy(pagina_info->tag, tag, MAX_FILETAG - 1);

        log_info(logger_worker, "Query %d: Acción: ESCRIBIR - DirFisica: %d - Pagina: %d", query_id, direccion_fisica, num_pagina);
        
        // 6. Write-Through (Enviar inmediatamente al Storage)
        // OJO: Esto manda UN bloque al storage. Como el Storage ya soporta recibir el bloque
        // y actualizarlo, esto está bien.
        t_op_storage* op_write = calloc(1, sizeof(t_op_storage));
        op_write->query_id = query_id;
        op_write->nombre_file = strdup(file);
        op_write->nombre_tag  = strdup(tag);
        op_write->direccion_base = num_pagina; 
        op_write->tamano_contenido = tam_pagina;
        op_write->contenido = malloc(tam_pagina);
        memcpy(op_write->contenido, memoria_principal + (marco * tam_pagina), tam_pagina);
        
        enviar_op_simple_storage(socket_storage, socket_master, WRITE, op_write);

        // 7. Avanzar punteros
        bytes_escritos += bytes_a_escribir_ahora;
        dir_logica_actual += bytes_a_escribir_ahora;
    }
}

char* leer_de_memoria(int query_id, const char* file, const char* tag, int direccion_logica, int tamanio, int socket_storage, int socket_master) {
    int num_pagina = direccion_logica / tam_pagina;
    int offset = direccion_logica % tam_pagina;

    // 1. Buscar si la página ya está en memoria
    int marco = buscar_pagina_en_memoria(file, tag, num_pagina);

    if (marco == -1) {
        // --- PAGE FAULT ---
        log_info(logger_worker, "## Query %d: (READ) - Memoria Miss - File: %s - Tag: %s - Pagina: %d", 
                 query_id, file, tag, num_pagina);
        
        marco = obtener_marco_libre();
        if (marco == -1) { // No hay marcos libres, hay que reemplazar
             marco = reemplazar_pagina(query_id, socket_storage, socket_master, file, tag, num_pagina);
        }

        // 2. Pedir el bloque al Storage
        log_info(logger_worker, "## Query %d: Solicitando bloque %d de %s:%s a Storage", query_id, num_pagina, file, tag);
        t_op_storage* op_read_req = calloc(1, sizeof(t_op_storage));
        op_read_req->query_id = query_id;
        op_read_req->nombre_file = strdup(file);
        op_read_req->nombre_tag  = strdup(tag);
        op_read_req->direccion_base = num_pagina; // nro_bloque_logico
        op_read_req->tamano = tam_pagina; 

        char* contenido_bloque = enviar_op_read_storage(socket_storage, socket_master, op_read_req);

        if (contenido_bloque == NULL) {
            log_error(logger_worker, "## Query %d: (READ) Page Fault falló. Storage no devolvió datos.", query_id);
            return NULL;
        }

        // 3. Cargar el bloque en memoria principal
        memcpy(memoria_principal + (marco * tam_pagina), contenido_bloque, tam_pagina);
        free(contenido_bloque);
        
        log_info(logger_worker, "## Query %d: Memoria Add - File: %s - Tag: %s Pagina: %d Marco: %d",
                 query_id, file, tag, num_pagina, marco);

        // 4. Actualizar flags de la nueva página
        PaginaMemoria* pagina_info = &tabla_de_marcos[marco];
        pagina_info->ocupado = true;
        pagina_info->modificado = false; // Acaba de ser cargada
        pagina_info->usado = true;       // Se va a usar
        pagina_info->timestamp = (unsigned long long)time(NULL); // Para LRU
        pagina_info->num_pagina = num_pagina;
        strncpy(pagina_info->file, file, MAX_FILETAG - 1);
        strncpy(pagina_info->tag, tag, MAX_FILETAG - 1);
    
    } else {
         log_info(logger_worker, "## Query %d: (READ) Memoria Hit - File: %s - Tag: %s Pagina: %d Marco: %d",
                 query_id, file, tag, num_pagina, marco);
        
         // Actualizar flags de la página existente
         PaginaMemoria* pagina_info = &tabla_de_marcos[marco];
         pagina_info->usado = true;
         pagina_info->timestamp = (unsigned long long)time(NULL); // Actualizar para LRU
    }

    // 5. Leer de la memoria (ahora sí está)
    int direccion_fisica = (marco * tam_pagina) + offset;
    usleep(worker_configs.retardomemoria * 1000);

    // Creamos un buffer para devolver el contenido leído.
    char* valor_leido = malloc(tamanio + 1);
    memcpy(valor_leido, memoria_principal + direccion_fisica, tamanio);
    valor_leido[tamanio] = '\0';
    
    log_info(logger_worker, "## Query %d: Acción: LEER - Dirección Física: %d - Valor: %s",
             query_id, direccion_fisica, valor_leido);

    return valor_leido;
}

// 1. Nueva función auxiliar para hacer FLUSH de páginas 
void realizar_flush_file(int query_id, const char* file, const char* tag, int socket_storage, int socket_master) {
    for (int i = 0; i < cantidad_marcos; i++) {
        // Si file/tag son NULL, flushea TODO (útil para desalojo)
        // Si tienen valor, solo flushea las páginas de ese archivo (útil para COMMIT)
        bool es_el_archivo = (file == NULL && tag == NULL) || 
                             (tabla_de_marcos[i].ocupado && 
                              strcmp(tabla_de_marcos[i].file, file) == 0 && 
                              strcmp(tabla_de_marcos[i].tag, tag) == 0);

        if (es_el_archivo && tabla_de_marcos[i].modificado) {
            log_info(logger_worker, "## Query %d: FLUSH Implícito Marco %d (File: %s Pagina: %d)", 
                     query_id, i, tabla_de_marcos[i].file, tabla_de_marcos[i].num_pagina);
            
            t_op_storage* op_flush = calloc(1, sizeof(t_op_storage));
            op_flush->query_id = query_id;
            op_flush->nombre_file = strdup(tabla_de_marcos[i].file);
            op_flush->nombre_tag = strdup(tabla_de_marcos[i].tag);
            op_flush->direccion_base = tabla_de_marcos[i].num_pagina;
            
            // bin
            op_flush->tamano_contenido = tam_pagina; // Escribimos toda la página
            op_flush->contenido = malloc(tam_pagina);
            memcpy(op_flush->contenido, memoria_principal + (i * tam_pagina), tam_pagina);

            enviar_op_simple_storage(socket_storage, socket_master, WRITE, op_flush);
            
            // Marcamos como limpio
            tabla_de_marcos[i].modificado = false; 
        }
    }
}
