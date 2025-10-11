#include "worker_memoria.h"
#include "worker.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

extern t_worker_config worker_configs; // Configuración global

static void* memoria_principal;
static PaginaMemoria* tabla_marcos;
static int cantidad_marcos;
static int tam_pagina;
static int puntero_clock = 0; // Para CLOCK-M

// ============================================================
// Inicialización
// ============================================================
void inicializar_memoria(int tam_mem, int tam_pag) {
    memoria_principal = malloc(tam_mem);
    cantidad_marcos = tam_mem / tam_pag;
    tam_pagina = tam_pag;

    tabla_marcos = calloc(cantidad_marcos, sizeof(PaginaMemoria));

    printf("Memoria inicializada: %d bytes (%d marcos de %d bytes)\n",
           tam_mem, cantidad_marcos, tam_pag);
}

// ============================================================
// Buscar marco libre
// ============================================================
int obtener_marco_libre() {
    for (int i = 0; i < cantidad_marcos; i++) {
        if (!tabla_marcos[i].ocupado) {
            return i;
        }
    }
    return -1;
}

// ============================================================
// Reemplazo de página (LRU o CLOCK-M)
// ============================================================
int reemplazar_pagina() {
    const char* algoritmo = worker_configs.algoritmoreemplazo;
    int victima = -1;

    if (strcmp(algoritmo, "LRU") == 0) {
        // Busco el marco con timestamp más viejo
        unsigned long long min_time = ULLONG_MAX;
        for (int i = 0; i < cantidad_marcos; i++) {
            if (tabla_marcos[i].ocupado && tabla_marcos[i].timestamp < min_time) {
                min_time = tabla_marcos[i].timestamp;
                victima = i;
            }
        }
    } else {
        // CLOCK-M
        while (true) {
            PaginaMemoria* p = &tabla_marcos[puntero_clock];
            if (p->ocupado) {
                if (!p->usado && !p->modificado) {
                    victima = puntero_clock;
                    break;
                } else if (!p->usado && p->modificado) {
                    victima = puntero_clock;
                    break;
                }
                p->usado = false; // Le damos una segunda oportunidad
            }
            puntero_clock = (puntero_clock + 1) % cantidad_marcos;
        }
    }

    PaginaMemoria* v = &tabla_marcos[victima];
    printf("## Query ?: Se reemplaza la página %s:%s/%d (marco %d)\n",
           v->file, v->tag, v->num_pagina, victima);

    // Si está modificada, habría que hacer un FLUSH al Storage aquí

    v->ocupado = false;
    v->modificado = false;
    v->usado = false;

    return victima;
}

// ============================================================
// Leer memoria
// ============================================================
void leer_memoria(const char* file, const char* tag, int direccion, int tamanio) {
    int num_pagina = direccion / tam_pagina;
    int marco = obtener_marco_libre();

    if (marco == -1) marco = reemplazar_pagina();

    // Simulamos acceso
    int direccion_fisica = marco * tam_pagina + (direccion % tam_pagina);
    printf("Query ?: Acción: LEER - Dirección Física: %d - Tamaño: %d\n", direccion_fisica, tamanio);
    usleep(worker_configs.retardomemoria * 1000);

    tabla_marcos[marco].usado = true;
    tabla_marcos[marco].timestamp = time(NULL);
}

// ============================================================
// Escribir memoria
// ============================================================
void escribir_memoria(const char* file, const char* tag, int direccion, const char* contenido) {
    int num_pagina = direccion / tam_pagina;
    int marco = obtener_marco_libre();

    if (marco == -1) marco = reemplazar_pagina();

    int direccion_fisica = marco * tam_pagina + (direccion % tam_pagina);
    memcpy(memoria_principal + direccion_fisica, contenido, strlen(contenido));

    printf("Query ?: Acción: ESCRIBIR - Dirección Física: %d - Valor: %s\n", direccion_fisica, contenido);

    PaginaMemoria* p = &tabla_marcos[marco];
    strcpy(p->file, file);
    strcpy(p->tag, tag);
    p->num_pagina = num_pagina;
    p->ocupado = true;
    p->modificado = true;
    p->usado = true;
    p->timestamp = time(NULL);

    printf("Query ?: Se asigna el Marco: %d a la Página: %d perteneciente al - File: %s - Tag: %s\n",
           marco, num_pagina, file, tag);

    usleep(worker_configs.retardomemoria * 1000);
}

// ============================================================
// Liberar memoria
// ============================================================
void liberar_memoria() {
    free(memoria_principal);
    free(tabla_marcos);
}