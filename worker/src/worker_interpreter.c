/*#include "worker_interpreter.h"
#include "worker_memoria.h"
#include "worker.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define MAX_LINEA 256

static void esperar_retardo() {
    usleep(worker_configs.retardomemoria * 1000);
}

static void log_fetch(int pc, const char* instruccion) {
    printf("## Query ?: FETCH - Program Counter: %d - %s\n", pc, instruccion);
}

void ejecutar_query(const char* path_query, int socket_master, int socket_storage, const char* id_worker) {
    FILE* archivo = fopen(path_query, "r");
    if (!archivo) {
        perror("Error abriendo query");
        return;
    }

    printf("## Query ?: Se recibe la Query. El path de operaciones es: %s\n", path_query);

    char linea[MAX_LINEA];
    int program_counter = 0;

    while (fgets(linea, sizeof(linea), archivo)) {
        // Eliminar salto de línea
        linea[strcspn(linea, "\n")] = 0;
        if (strlen(linea) == 0) continue; // Saltar líneas vacías

        log_fetch(program_counter++, linea);

        // Tokenizar
        char* instruccion = strtok(linea, " ");
        if (!instruccion) continue;

        // ==== CREATE ====
        if (strcmp(instruccion, "CREATE") == 0) {
            char* file_tag = strtok(NULL, " ");
            printf("## Query ?: - Instrucción realizada: CREATE %s\n", file_tag);
            enviar_mensaje("CREATE", socket_storage);
        }

        // ==== TRUNCATE ====
        else if (strcmp(instruccion, "TRUNCATE") == 0) {
            char* file_tag = strtok(NULL, " ");
            char* tamanio = strtok(NULL, " ");
            printf("## Query ?: - Instrucción realizada: TRUNCATE %s %s\n", file_tag, tamanio);
            enviar_mensaje("TRUNCATE", socket_storage);
        }

        // ==== WRITE ====
        else if (strcmp(instruccion, "WRITE") == 0) {
            char* file_tag = strtok(NULL, " ");
            char* direccion = strtok(NULL, " ");
            char* contenido = strtok(NULL, " ");
            char* file = strtok(file_tag, ":");
            char* tag = strtok(NULL, ":");

            escribir_memoria(file, tag, atoi(direccion), contenido);
            printf("## Query ?: - Instrucción realizada: WRITE %s:%s %s %s\n",
                   file, tag, direccion, contenido);
        }

        // ==== READ ====
        else if (strcmp(instruccion, "READ") == 0) {
            char* file_tag = strtok(NULL, " ");
            char* direccion = strtok(NULL, " ");
            char* tamanio = strtok(NULL, " ");
            char* file = strtok(file_tag, ":");
            char* tag = strtok(NULL, ":");

            leer_memoria(file, tag, atoi(direccion), atoi(tamanio));
            printf("## Query ?: - Instrucción realizada: READ %s:%s %s %s\n",
                   file, tag, direccion, tamanio);

            // Aquí podrías enviar el contenido leído al Master
            // enviar_mensaje("Resultado de READ", socket_master);
        }

        // ==== TAG ====
        else if (strcmp(instruccion, "TAG") == 0) {
            printf("## Query ?: - Instrucción realizada: TAG\n");
            enviar_mensaje("TAG", socket_storage);
        }

        // ==== COMMIT ====
        else if (strcmp(instruccion, "COMMIT") == 0) {
            printf("## Query ?: - Instrucción realizada: COMMIT\n");
            enviar_mensaje("COMMIT", socket_storage);
        }

        // ==== FLUSH ====
        else if (strcmp(instruccion, "FLUSH") == 0) {
            printf("## Query ?: - Instrucción realizada: FLUSH\n");
            // (Aquí se persistiría lo modificado)
        }

        // ==== DELETE ====
        else if (strcmp(instruccion, "DELETE") == 0) {
            printf("## Query ?: - Instrucción realizada: DELETE\n");
            enviar_mensaje("DELETE", socket_storage);
        }

        // ==== END ====
        else if (strcmp(instruccion, "END") == 0) {
            printf("## Query ?: - Instrucción realizada: END\n");
            enviar_mensaje("FIN_QUERY", socket_master);
            break;
        }

        esperar_retardo();
    }

    fclose(archivo);
}

*/
//la interpretacion ya esta en main, pero se puede volver a usar esto, solo lo comente para que compile