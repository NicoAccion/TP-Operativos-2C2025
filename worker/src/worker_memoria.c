#include "worker_memoria.h"
#include "worker-configs.h"
#include "worker-log.h"
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
//static int puntero_clock = 0;

void inicializar_memoria(int tam_mem, int tam_pag) {
    memoria_principal = malloc(tam_mem);
    cantidad_marcos = tam_mem / tam_pag;
    tam_pagina = tam_pag;
    tabla_de_marcos = calloc(cantidad_marcos, sizeof(PaginaMemoria));

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

static int reemplazar_pagina(int query_id) {
    int marco_victima = -1;
    

    // TODO: lógica de LRU o CLOCK-M.
    
    // Simulación simple: elegimos el marco 0 como víctima.
    marco_victima = 0; 
    PaginaMemoria* victima = &tabla_de_marcos[marco_victima];

    log_info(logger_worker, "## Query %d: Reemplazo - Página candidata: File %s:%s Pagina %d en Marco %d", 
             query_id, victima->file, victima->tag, victima->num_pagina, marco_victima);

    if (victima->modificado) {
        log_info(logger_worker, "La página víctima está modificada. Se debería escribir a Storage (FLUSH).");
        // Aquí iría la lógica para enviar el contenido del marco a Storage.
    }
    
    return marco_victima;
}

void escribir_en_memoria(int query_id, const char* file, const char* tag, int direccion_logica, const char* contenido) {
    int num_pagina = direccion_logica / tam_pagina;
    int offset = direccion_logica % tam_pagina;
    
    // Por ahora, no implementamos tabla de páginas por File:Tag,
    // simplemente buscamos un marco para la página.
    int marco = obtener_marco_libre();
    if (marco == -1) {
        marco = reemplazar_pagina(query_id);
    }
    
    int direccion_fisica = (marco * tam_pagina) + offset;
    memcpy(memoria_principal + direccion_fisica, contenido, strlen(contenido) + 1);

    usleep(worker_configs.retardomemoria * 1000);

    log_info(logger_worker, "## Query %d: Acción: ESCRIBIR - Dirección Física: %d - Valor: %s",
             query_id, direccion_fisica, contenido);

    // Actualizamos la información administrativa del marco
    PaginaMemoria* pagina_info = &tabla_de_marcos[marco];
    pagina_info->ocupado = true;
    pagina_info->modificado = true;
    pagina_info->usado = true;
    pagina_info->num_pagina = num_pagina;
    strncpy(pagina_info->file, file, MAX_FILETAG - 1);
    strncpy(pagina_info->tag, tag, MAX_FILETAG - 1);

    log_info(logger_worker, "## Query %d: Se asigna el Marco: %d a la Página: %d perteneciente al File: %s Tag: %s",
             query_id, marco, num_pagina, file, tag);
}

char* leer_de_memoria(int query_id, const char* file, const char* tag, int direccion_logica, int tamanio) {
    int num_pagina = direccion_logica / tam_pagina;
    int offset = direccion_logica % tam_pagina;

    // Para check2 simulacion
    log_info(logger_worker, "## Query %d: Memoria Miss - File: %s - Tag: %s Pagina: %d", 
             query_id, file, tag, num_pagina);
    
    int marco = obtener_marco_libre();
    if (marco == -1) {
        marco = reemplazar_pagina(query_id);
    }

    // Simulamos que pedimos el bloque a Storage y lo cargamos.
    log_info(logger_worker, "## Query %d: Memoria Add - File: %s - Tag: %s Pagina: %d Marco: %d",
             query_id, file, tag, num_pagina, marco);

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