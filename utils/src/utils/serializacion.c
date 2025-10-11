#include "serializacion.h"

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                        Funciones de buffer

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

t_buffer *buffer_create(uint32_t size) {
    t_buffer *buffer = malloc(sizeof(t_buffer));
    buffer->size = size;
    buffer->offset = 0;
    buffer->stream = malloc(size); //Recordar liberar
    return buffer;
}

void buffer_destroy(t_buffer *buffer) {
    if(buffer){
        free(buffer->stream);
        free(buffer);
    }
}

void buffer_add(t_buffer *buffer, void *data, uint32_t size) {
    if(buffer->offset + size > buffer->size){
        printf("Error: Buffer overflow\n");
        exit(1);
    }
    memcpy(buffer->stream + buffer->offset, data, size);
    buffer->offset += size;
}

void buffer_read(t_buffer *buffer, void *data, uint32_t size) {
    memcpy(data, buffer->stream + buffer->offset, size);
    buffer->offset += size;
}

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                        Funciones de buffer especificas

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

void buffer_add_uint32(t_buffer *buffer, uint32_t data) {
    buffer_add(buffer, &data, sizeof(uint32_t));
}

uint32_t buffer_read_uint32(t_buffer *buffer) {
    uint32_t data;
    buffer_read(buffer, &data, sizeof(uint32_t));
    return data;
}

void buffer_add_string(t_buffer *buffer, uint32_t length, char *string) {
    buffer_add_uint32(buffer, length);
    buffer_add(buffer, string, length);
}

char *buffer_read_string(t_buffer *buffer, uint32_t *length) {
    *length = buffer_read_uint32(buffer);
    char *string = malloc(*length + 1); //Recordar liberar
    buffer_read(buffer, string, *length);
    string[*length] = '\0';
    return string;
}

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                            Función de empaquetado

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

t_paquete* empaquetar_buffer(t_codigo_operacion codigo_operacion, t_buffer* buffer) {
    t_paquete* paquete = malloc(sizeof(t_paquete));
    paquete->codigo_operacion = codigo_operacion;
    paquete->buffer = buffer;
    return paquete;
}

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                        Función de armado de streams, para enviar

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

void* stream_para_enviar(t_paquete* paquete){
    void* stream = malloc(paquete->buffer->size + sizeof(t_codigo_operacion) + sizeof(uint32_t));
    int offset = 0;
    memcpy(stream + offset, &(paquete->codigo_operacion), sizeof(t_codigo_operacion));
    offset += sizeof(t_codigo_operacion);

    memcpy(stream + offset, &(paquete->buffer->size), sizeof(uint32_t));
    offset += sizeof(uint32_t);

    memcpy(stream + offset, paquete->buffer->stream, paquete->buffer->size);
    return stream; //Recordar liberar en donde se llame
}

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                        Funciones de enviado y recepcion de paquetes

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

int enviar_paquete(int socket, t_paquete* paquete){
    //Arma el stream a partir del paquete, que contiene un codigo de operacion y un buffer
    void* stream = stream_para_enviar(paquete);
    //Envia el stream al socket recibido por parametro
    if(send(socket, stream, paquete->buffer->size + sizeof(t_codigo_operacion) + sizeof(uint32_t), 0) == -1){
        liberar_paquete(paquete); //Libera el paquete recibido por parametro porque fallo el envio
        free(stream); //Libera el stream creado despues de enviarlo porque fallo el envio
        return -1;
    };
    liberar_paquete(paquete); //Libera el paquete recibido por parametro despues de enviarlo
    free(stream); //Libera el stream creado despues de enviarlo
    return 0;
}

t_paquete* recibir_paquete(int socket){
    t_paquete* paquete = malloc(sizeof(t_paquete));
    if(paquete == NULL){
        return NULL;
    }
    paquete->buffer = malloc(sizeof(t_buffer));
    if(paquete->buffer == NULL){
        free(paquete);
        return NULL;
    }

    //Recibir codigo de operacion
    if (recv(socket, &(paquete->codigo_operacion), sizeof(t_codigo_operacion), MSG_WAITALL) <= 0) {
        free(paquete->buffer);
        free(paquete);
        return NULL;
    }
    
    //Recibir tamaño del buffer
    if (recv(socket, &(paquete->buffer->size), sizeof(uint32_t), MSG_WAITALL) <= 0) {
        free(paquete->buffer);
        free(paquete);
        return NULL;
    }
    
    if (paquete->buffer->size > 0) {
        paquete->buffer->stream = malloc(paquete->buffer->size);
        if (paquete->buffer->stream == NULL) {
             free(paquete->buffer);
             free(paquete);
             return NULL;
        }
        if (recv(socket, paquete->buffer->stream, paquete->buffer->size, MSG_WAITALL) <= 0) {
            free(paquete->buffer->stream);
            free(paquete->buffer);
            free(paquete);
            return NULL;
        }
    } else {
        paquete->buffer->stream = NULL;
    }
    paquete->buffer->offset = 0;

    return paquete; //Recordar liberar en donde se llame
}

void liberar_paquete(t_paquete* paquete){
    free(paquete->buffer->stream);
    free(paquete->buffer);
    free(paquete);
}

/*/////////////////////////////////////////////////////////////////////////////////////////////////////////////

                                    Funciones de serializacion y deserializacion

/////////////////////////////////////////////////////////////////////////////////////////////////////////////*/

t_buffer* serializar_query(t_query* query){
    uint32_t tamanio = 2 * sizeof(uint32_t) + strlen(query->archivo_query);
    t_buffer* buffer = buffer_create(tamanio);
    buffer_add_string(buffer, strlen(query->archivo_query), query->archivo_query);
    buffer_add_uint32(buffer, query->prioridad);
    return buffer;
}

t_query* deserializar_query(t_buffer* buffer){
    t_query* query = malloc(sizeof(t_query));
    uint32_t length;
    query->archivo_query = buffer_read_string(buffer, &length);
    query->prioridad = buffer_read_uint32(buffer);
    return query;
}


t_buffer* serializar_operacion_query(t_operacion_query* operacion_query){
    uint32_t tamanio = 3 * sizeof(uint32_t) + strlen(operacion_query->informacion) + 
    strlen(operacion_query->file) + strlen(operacion_query->tag);
    t_buffer* buffer = buffer_create(tamanio);
    buffer_add_string(buffer, strlen(operacion_query->informacion), operacion_query->informacion);
    buffer_add_string(buffer, strlen(operacion_query->file), operacion_query->file);
    buffer_add_string(buffer, strlen(operacion_query->tag), operacion_query->tag);
    return buffer;
}

t_operacion_query* deserializar_operacion_query(t_buffer* buffer){
    t_operacion_query* operacion_query = malloc(sizeof(t_operacion_query));
    uint32_t length;
    operacion_query->informacion = buffer_read_string(buffer, &length);
    operacion_query->file = buffer_read_string(buffer, &length);
    operacion_query->tag = buffer_read_string(buffer, &length);
    return operacion_query;
}


t_buffer* serializar_worker(uint32_t id) {
    uint32_t tamanio = sizeof(uint32_t);
    t_buffer* buffer = buffer_create(tamanio);
    buffer_add_uint32(buffer, id);
    return buffer;
}

uint32_t deserializar_worker(t_buffer* buffer) {
    uint32_t id = buffer_read_uint32(buffer);
    return id;
}


t_buffer* serializar_query_completa(t_query_completa* query_completa){
    uint32_t tamanio = 4 * sizeof(uint32_t) + strlen(query_completa->archivo_query);
    t_buffer* buffer = buffer_create(tamanio);
    buffer_add_string(buffer, strlen(query_completa->archivo_query), query_completa->archivo_query);
    buffer_add_uint32(buffer, query_completa->prioridad);
    buffer_add_uint32(buffer, query_completa->id_query);
    buffer_add_uint32(buffer, query_completa->estado);
    return buffer;
}

t_query_completa* deserializar_query_completa(t_buffer* buffer){
    t_query_completa* query_completa = malloc(sizeof(t_query_completa));
    uint32_t length;
    query_completa->archivo_query = buffer_read_string(buffer, &length);
    query_completa->prioridad = buffer_read_uint32(buffer);
    query_completa->id_query = buffer_read_uint32(buffer);
    query_completa->estado = buffer_read_uint32(buffer);
    return query_completa;
}

void destruir_operacion_query(t_operacion_query* op) {
    if (op == NULL) return;

    free(op->informacion);
    free(op->file);
    free(op->tag);
    free(op);
}