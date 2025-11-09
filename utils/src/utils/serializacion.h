#ifndef SERIALIZACION_H
#define SERIALIZACION_H

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <unistd.h>

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                        Estructuras usadas

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/


/**
 * @enum t_codigo_operacion
 * @brief Enum que representa los códigos de operación para los paquetes
 * 
 * @param PAQUETE_QUERY: Código de operación para enviar un archivo query con su prioridad
 * @param PAQUETE_WORKER: Código de operación para enviar el id worker
 * @param PAQUETE_QUERY_COMPLETA: Código de operación para enviar una query completa
 * @param READ: Código de operación para enviar el resultado de una lectura
 * @param END: Código de operación para enviar el fin de una query
 */
typedef enum {
    HANDSHAKE_QUERYCONTROL = 1,
    HANDSHAKE_WORKER = 2,
    PAQUETE_QUERY_EJECUCION = 3,
    READ = 4,
    END = 5,
    CREATE = 6,
    TRUNCATE = 7,
    DELETE = 8,
    COMMIT = 9,
    TAG = 10,
    DESCONEXION_QUERY = 11,
    DESALOJO_PRIORIDADES = 12,
    WRITE = 13,
    FLUSH_PAGINA,
    //Codigos de respuesta del storage
    HANDSAHKE_STORAGE_RTA,
    OP_OK,
    OP_ERROR,
    READ_RTA,
} t_codigo_operacion;


/**
 * @struct t_buffer
 * @brief Estructura que representa un buffer para serialización y deserialización. 
 * Contiene un stream(el payload), el tamaño del payload y un offset que representa la posicion actual en el stream.
 * 
 * @param size: Tamaño del payload
 * @param offset: Desplazamiento dentro del payload
 * @param stream: Payload
 */
typedef struct {
    uint32_t size; // Tamaño del payload
    uint32_t offset; // Desplazamiento dentro del payload
    void* stream; // Payload
} t_buffer;


/**
 * @struct t_paquete
 * @brief Estructura que representa un paquete para enviar y recibir paquetes.
 * Contiene un codigo de operacion (que sirve para saber como deserializar el buffer) y un buffer.
 * 
 * @param codigo_operacion: Código de operación para el paquete
 * @param buffer: Buffer que contiene el payload
 */
typedef struct {
    t_codigo_operacion codigo_operacion;
    t_buffer* buffer;
} t_paquete;


/**
 * @struct t_query
 * @brief Estructura que representa un archivo query
 * 
 * @param archivo_query: path del archivo de la query
 * @param prioridad: prioridad de la query
 */
typedef struct {
    char* archivo_query;
    uint32_t prioridad;
} t_query;

/**
 * @struct t_operacion_query
 * @brief Estructura que representa la información leída por la query
 * 
 * @param informacion: Información leída
 * @param file: Nombre del file
 * @param tag: Nombre del tag
 */
typedef struct {
    char* informacion;
    char* file;
    char* tag;
} t_operacion_query;

/**
 * @enum t_estado
 * @brief Enum que representa el estado en que se encuentra una query
 * 
 * @param READY: La query se encuentra en estado READY
 * @param EXEC: La query se encuentra en estado EXEC
 * @param EXIT: La query se encuentra en estado EXIT
 */
typedef enum {
    READY,
    EXEC,
    EXIT,
} t_estado;

/**
 * @struct t_query_ejecucion
 * @brief Estructura que representa una query en ejecucion
 * 
 * @param archivo_query: path del archivo de la query
 * @param id_query: id de la query
 * @param program_counter: program counter de la query
 */
typedef struct {
    char* archivo_query;
    uint32_t id_query;
    uint32_t program_counter; 
} t_query_ejecucion;


/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                        Funciones de buffer

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
 * @brief Crea un buffer vacío de tamaño size y offset 0
 * 
 * @param size Tamaño del buffer
 * @return t_buffer* Puntero al buffer creado
 */
t_buffer *buffer_create(uint32_t size);


/**
 * @brief Libera la memoria asociada al buffer
 * 
 * @param buffer Puntero al buffer a liberar
 */
void buffer_destroy(t_buffer *buffer);


/**
 * @brief Agrega un stream al buffer en la posición actual y avanza el offset
 * 
 * @param buffer Puntero al buffer
 * @param data Puntero al stream a agregar
 * @param size Tamaño del stream a agregar
 */
void buffer_add(t_buffer *buffer, void *data, uint32_t size);


/**
 * @brief Guarda size bytes del principio del buffer en la dirección data y avanza el offset
 * 
 * @param buffer Puntero al buffer
 * @param data Puntero a la dirección donde se guardarán los datos
 * @param size Tamaño de los datos a guardar
 */
void buffer_read(t_buffer *buffer, void *data, uint32_t size);


/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                        Funciones de buffer especificas

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

// Agrega un uint32_t al buffer
void buffer_add_uint32(t_buffer *buffer, uint32_t data);

// Lee un uint32_t del buffer y avanza el offset
uint32_t buffer_read_uint32(t_buffer *buffer);

// Agrega string al buffer con un uint32_t adelante indicando su longitud
void buffer_add_string(t_buffer *buffer, uint32_t length, char *string);

// Lee un string y su longitud del buffer y avanza el offset
char *buffer_read_string(t_buffer *buffer, uint32_t *length);


/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                    Funciones de empaquetado y desempaquetado

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

t_paquete* empaquetar_buffer(t_codigo_operacion codigo_operacion, t_buffer* buffer);


/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                            Funciones de armado de streams

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

void* stream_para_enviar(t_paquete* paquete);


/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                    Funciones de enviado y recepcion de paquetes

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
 * @brief Recibe un paquete, arma un stream con los contenidos del paquete y lo envia a un socket. Esta funcion ya libera el paquete despues de enviarlo.
 * 
 * @param socket Descriptor del socket
 * @param paquete Puntero al paquete a enviar
 * @return int 0 si el envío fue exitoso, -1 si hubo un error
 */
int enviar_paquete(int socket, t_paquete* paquete);

/**
 * @brief Recibe un stream que fue armado a partir de un paquete, lo desarma, pone los contenidos en un paquete y lo devuelve
 * 
 * @param socket Descriptor del socket
 * @return t_paquete* Puntero al paquete recibido
 */
t_paquete* recibir_paquete(int socket);

/**
 * @brief Libera la memoria de un paquete
 * 
 * @param paquete Puntero al paquete a liberar
 */
void liberar_paquete(t_paquete* paquete);


/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                    Funciones de serializacion y deserializacion

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/


/**
 * @brief Recibe una query y la serializa
 * 
 * @param query Puntero a la estructura de la query
 * @return t_buffer* Puntero al buffer que contiene la query
 */
t_buffer* serializar_query(t_query* query);

/**
 * @brief Recibe un buffer y lo deserializa en una query
 * 
 * @param buffer Puntero al buffer que contiene la query
 * @return t_query* Puntero a la estructura de la query
 */
t_query* deserializar_query(t_buffer* buffer);


/**
 * @brief Recibe una operacion query y la serializa
 * 
 * @param operacion_query Puntero a la estructura de la operacion query
 * @return t_buffer* Puntero al buffer que contiene la operacion query
 */
t_buffer* serializar_operacion_query(t_operacion_query* operacion_query);


/**
 * @brief Recibe un buffer y lo deserializa en una operacion query
 * 
 * @param buffer Puntero al buffer que contiene la operacion query
 * @return t_operacion_query* Puntero a la estructura de la operacion query
 */
t_operacion_query* deserializar_operacion_query(t_buffer* buffer);

/**
 * @brief Recibe el motivo de un END y lo serializa
 * 
 * @param operacion_end Motivo por el que se envió un END
 * @return t_buffer* Puntero al buffer que contiene la operacion read
 */
t_buffer* serializar_operacion_end(char* operacion_end);

/**
 * @brief Recibe un buffer y lo deserializa en un motivo de END
 * 
 * @param buffer Puntero al buffer que contiene el motivo de END
 * @return char* Motivo del END 
 */
char* deserializar_operacion_end(t_buffer* buffer);


/**
 * @brief Recibe un id worker y lo serializa
 * 
 * @param id El id worker
 * @return t_buffer* Puntero al buffer que contiene el id worker
 */
t_buffer* serializar_worker(uint32_t id);

/**
 * @brief Recibe un buffer y lo deserializa en un id worker
 * 
 * @param buffer Puntero al buffer que contiene el id worker
 * @return uint32_t El id worker
 */
uint32_t deserializar_worker(t_buffer* buffer);


/**
 * @brief Recibe una t_query_ejecucion y la serializa
 * 
 * @param query Puntero a la estructura t_query_ejecucion
 * @return t_buffer* Puntero al buffer que contiene la t_query_ejecucion
 */
t_buffer* serializar_query_ejecucion(t_query_ejecucion* query);

/**
 * @brief Recibe un buffer y lo deserializa en una t_query_ejecucion
 * 
 * @param buffer Puntero al buffer que contiene la t_query_ejecucion
 * @return t_query_ejecucion* Puntero a la estructura t_query_ejecucion
 */
t_query_ejecucion* deserializar_query_ejecucion(t_buffer* buffer);


void destruir_operacion_query(t_operacion_query* op);


/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                            Funciones de serializacion y deserializacion (Worker <-> Storage)

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

/**
 * @struct t_op_storage
 * @brief Estructura genérica para todas las operaciones que el Worker solicita al Storage.
 * NO todos los campos se usan en TODAS las operaciones.
 */
typedef struct {
    uint32_t query_id;
    char* nombre_file;
    char* nombre_tag;
    uint32_t tamano; // Para TRUNCATE y READ
    uint32_t direccion_base; // Para WRITE y READ
    char* contenido; // Para WRITE
    char* nombre_file_destino; // Para TAG
    char* nombre_tag_destino; // Para TAG
} t_op_storage;


/**
 * @brief Serializa una operacion genérica de Storage
 * @param op Puntero a la estructura t_op_storage
 * @param codigo_operacion El código (CREATE, TRUNCATE, etc.) para saber qué campos serializar
 * @return t_buffer* Puntero al buffer serializado
 */
t_buffer* serializar_op_storage(t_op_storage* op, t_codigo_operacion codigo_operacion);

/**
 * @brief Deserializa una operacion genérica de Storage
 * @param buffer Puntero al buffer
 * @param codigo_operacion El código (CREATE, TRUNCATE, etc.) para saber qué campos deserializar
 * @return t_op_storage* Puntero a la nueva estructura
 */
t_op_storage* deserializar_op_storage(t_buffer* buffer, t_codigo_operacion codigo_operacion);

/**
 * @brief Libera la memoria de una t_op_storage
 */
void destruir_op_storage(t_op_storage* op);


#endif