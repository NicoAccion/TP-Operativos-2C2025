// storage.c
#define _GNU_SOURCE
#include "storage.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <openssl/md5.h>
#include <stdint.h>
#include <time.h>

#define BACKLOG 10
#define MAX_CLIENT_MSG 4096

// ========== Globals ==========
t_storage_config storage_cfg;
pthread_mutex_t mutex_bitmap = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_hashindex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_workers_count = PTHREAD_MUTEX_INITIALIZER;
int workers_connected = 0;

// bitarray stored as bytes in memory (persisted to bitmap.bin)
uint8_t *bitmap = NULL;
size_t bitmap_bytes = 0;

char path_superblock[MAX_PATH];
char path_bitmap[MAX_PATH];
char path_hashindex[MAX_PATH];
char path_physical[MAX_PATH];
char path_files[MAX_PATH];

// ========== Utilidades ==========

static void trim(char* s){
    char *p = s;
    while(*p && (*p==' '||*p=='\t' || *p=='\r' || *p=='\n')) p++;
    if (p != s) memmove(s, p, strlen(p)+1);
    // trim end
    int len = strlen(s);
    while (len>0 && (s[len-1]==' '||s[len-1]=='\t'||s[len-1]=='\r'||s[len-1]=='\n')) s[--len]=0;
}

void leer_config_storage(const char* path_config, t_storage_config* cfg){
    FILE* f = fopen(path_config, "r");
    if (!f) {
        perror("leer_config_storage fopen");
        exit(EXIT_FAILURE);
    }
    char line[512];
    // defaults
    cfg->puerto_escucha = 9002;
    cfg->fresh_start = true;
    strcpy(cfg->punto_montaje, "./storage_mount");
    cfg->retardo_operacion = 8000;
    cfg->retardo_acceso_bloque = 4000;
    cfg->fs_size = 4096;
    cfg->block_size = 128;
    while(fgets(line, sizeof(line), f)){
        trim(line);
        if (strlen(line)==0 || line[0]=='#') continue;
        char* eq = strchr(line,'=');
        if (!eq) continue;
        *eq = 0;
        char* key = line;
        char* val = eq+1;
        trim(key); trim(val);
        if (strcmp(key,"PUERTO_ESCUCHA")==0) cfg->puerto_escucha = atoi(val);
        else if (strcmp(key,"FRESH_START")==0) cfg->fresh_start = (strcasecmp(val,"TRUE")==0);
        else if (strcmp(key,"PUNTO_MONTAJE")==0) strncpy(cfg->punto_montaje, val, MAX_PATH-1);
        else if (strcmp(key,"RETARDO_OPERACION")==0) cfg->retardo_operacion = atoi(val);
        else if (strcmp(key,"RETARDO_ACCESO_BLOQUE")==0) cfg->retardo_acceso_bloque = atoi(val);
        else if (strcmp(key,"FS_SIZE")==0) cfg->fs_size = atoi(val);
        else if (strcmp(key,"BLOCK_SIZE")==0) cfg->block_size = atoi(val);
    }
    fclose(f);
    cfg->cantidad_bloques = cfg->fs_size / cfg->block_size;
}

// create directory if not exists
static void mkpath(const char* path){
    struct stat st;
    if (stat(path, &st) == 0) return;
    if (mkdir(path, 0755) == -1){
        if (errno != EEXIST) {
            fprintf(stderr, "mkpath %s errno %d\n", path, errno);
            exit(EXIT_FAILURE);
        }
    }
}

// ========== Bitmap handling ==========
static void bitmap_init_in_memory(size_t blocks){
    bitmap_bytes = (blocks + 7) / 8;
    bitmap = calloc(bitmap_bytes, 1);
    if (!bitmap) { perror("bitmap alloc"); exit(EXIT_FAILURE); }
}

static void bitmap_load_or_create(const char* path, size_t blocks, bool fresh){
    if (fresh) {
        bitmap_init_in_memory(blocks);
        // set all to 0
    } else {
        FILE* f = fopen(path, "rb");
        if (!f) {
            // create new empty
            bitmap_init_in_memory(blocks);
            return;
        }
        size_t read = fread(bitmap,1,bitmap_bytes,f);
        if (read != bitmap_bytes) {
            // if file smaller, extend
            memset(bitmap,0,bitmap_bytes);
        }
        fclose(f);
    }
}

static void bitmap_save(const char* path){
    pthread_mutex_lock(&mutex_bitmap);
    FILE* f = fopen(path, "wb");
    if (!f) { perror("bitmap_save fopen"); pthread_mutex_unlock(&mutex_bitmap); return; }
    fwrite(bitmap,1,bitmap_bytes,f);
    fclose(f);
    pthread_mutex_unlock(&mutex_bitmap);
}

static inline void set_bit(size_t index, int val){
    size_t byte_i = index / 8;
    int bit = index % 8;
    if (val) bitmap[byte_i] |= (1 << bit);
    else bitmap[byte_i] &= ~(1 << bit);
}

static inline int get_bit(size_t index){
    size_t byte_i = index / 8;
    int bit = index % 8;
    return (bitmap[byte_i] >> bit) & 1;
}

// find first free block or -1
static int find_first_free_block(){
    for (size_t i=0;i< (size_t)storage_cfg.cantidad_bloques;i++){
        if (!get_bit(i)) return (int)i;
    }
    return -1;
}

// ========== Hash index management ==========
/* blocks_hash_index.config format:
   md5hash=blockNNNN
*/
static void ensure_file_exists(const char* p){
    FILE* f = fopen(p,"a");
    if (!f) { perror("ensure_file_exists"); exit(EXIT_FAILURE); }
    fclose(f);
}

static void persist_hash_index_add(const char* md5, int blocknum){
    pthread_mutex_lock(&mutex_hashindex);
    FILE* f = fopen(path_hashindex,"a");
    if (!f) { perror("persist_hash_index_add"); pthread_mutex_unlock(&mutex_hashindex); return; }
    fprintf(f,"%s=block%05d\n", md5, blocknum);
    fclose(f);
    pthread_mutex_unlock(&mutex_hashindex);
}

// find block by md5; return -1 if not found
static int find_block_by_hash(const char* md5){
    pthread_mutex_lock(&mutex_hashindex);
    FILE* f = fopen(path_hashindex,"r");
    if (!f) { pthread_mutex_unlock(&mutex_hashindex); return -1; }
    char line[512];
    while(fgets(line,sizeof(line),f)){
        trim(line);
        if (strlen(line)==0) continue;
        char* eq = strchr(line,'=');
        if (!eq) continue;
        *eq = 0;
        char* key = line;
        char* val = eq+1;
        trim(key); trim(val);
        if (strcmp(key, md5) == 0){
            int blocknum = -1;
            if (sscanf(val,"block%05d",&blocknum) == 1) {
                fclose(f); pthread_mutex_unlock(&mutex_hashindex); return blocknum;
            }
        }
    }
    fclose(f);
    pthread_mutex_unlock(&mutex_hashindex);
    return -1;
}

// compute md5 hex string of buffer and length
static char* calcular_md5_hex_from_buf(const void* buf, size_t len){
    unsigned char md[MD5_DIGEST_LENGTH];
    MD5((const unsigned char*)buf, len, md);
    char* s = malloc(33);
    for (int i=0;i<MD5_DIGEST_LENGTH;i++) sprintf(s + i*2, "%02x", md[i]);
    s[32]=0;
    return s;
}

// ========== Helpers for logical/physical blocks and metadata ==========

static void path_for_blockfile(int blocknum, char* out, size_t outlen){
    snprintf(out, outlen, "%s/block%05d.dat", path_physical, blocknum);
}

static void ensure_physical_block_exists(int blocknum, int blocksize){
    char p[MAX_PATH];
    path_for_blockfile(blocknum,p,sizeof(p));
    FILE* f = fopen(p,"rb");
    if (f) { fclose(f); return; }
    f = fopen(p,"wb");
    if (!f) { perror("ensure_physical_block_exists fopen"); return; }
    // fill with zeros (blocksize bytes)
    void* z = calloc(1, blocksize);
    fwrite(z,1,blocksize,f);
    free(z);
    fclose(f);
}

// metadata.path = PUNTO_MONTAJE/files/<file>/<tag>/metadata.config
static void path_for_tag_metadata(const char* file, const char* tag, char* out, size_t outlen){
    snprintf(out, outlen, "%s/%s/%s/metadata.config", path_files, file, tag);
}

// logical block path: .../logical_blocks/000000.dat (hard link to physical)
static void path_for_logical_block(const char* file, const char* tag, int logical_idx, char* out, size_t outlen){
    snprintf(out, outlen, "%s/%s/%s/logical_blocks/%06d.dat", path_files, file, tag, logical_idx);
}

// read metadata file: expects keys TAMAÑO and BLOCKS and ESTADO
// returns 0 on success, -1 on not found
typedef struct {
    int tam_bytes;
    int estado_commited; // 0=WORK_IN_PROGRESS, 1=COMMITED
    int* blocks; // dynamic array of block numbers, caller must free
    int blocks_count;
} metadata_t;

static int read_metadata(const char* file, const char* tag, metadata_t* m){
    char p[MAX_PATH];
    path_for_tag_metadata(file, tag, p, sizeof(p));
    FILE* f = fopen(p,"r");
    if (!f) return -1;
    char line[1024];
    m->tam_bytes = 0;
    m->estado_commited = 0;
    m->blocks = NULL;
    m->blocks_count = 0;
    while(fgets(line,sizeof(line),f)){
        trim(line);
        if (strlen(line)==0) continue;
        if (strncmp(line,"TAMAÑO=",7)==0){
            m->tam_bytes = atoi(line+7);
        } else if (strncmp(line,"ESTADO=",7)==0){
            if (strstr(line+7,"COMMITED")) m->estado_commited = 1;
            else m->estado_commited = 0;
        } else if (strncmp(line,"BLOCKS=",7)==0){
            // parse like [17,2,5]
            char* s = strchr(line,'[');
            char* e = strchr(line,']');
            if (s && e && e > s){
                char tmp[1024]; memset(tmp,0,sizeof(tmp));
                int len = e - s - 1;
                if (len>0 && len < (int)sizeof(tmp)) {
                    strncpy(tmp, s+1, len);
                    // tokenize by comma
                    char* tok = strtok(tmp,",");
                    while(tok){
                        trim(tok);
                        int b = atoi(tok);
                        m->blocks = realloc(m->blocks, sizeof(int)*(m->blocks_count+1));
                        m->blocks[m->blocks_count++] = b;
                        tok = strtok(NULL, ",");
                    }
                }
            }
        }
    }
    fclose(f);
    return 0;
}

static int write_metadata(const char* file, const char* tag, metadata_t* m){
    char p[MAX_PATH];
    path_for_tag_metadata(file, tag, p, sizeof(p));
    // ensure dir exists
    char dir[MAX_PATH];
    snprintf(dir, sizeof(dir), "%s/%s/%s", path_files, file, tag);
    mkpath(path_files);
    // ensure file dir
    char dirfile[MAX_PATH];
    snprintf(dirfile, sizeof(dirfile), "%s/%s", path_files, file);
    mkpath(dirfile);
    mkpath(dir);
    // write metadata.config
    FILE* f = fopen(p,"w");
    if (!f) { perror("write_metadata fopen"); return -1; }
    fprintf(f,"TAMAÑO=%d\n", m->tam_bytes);
    fprintf(f,"ESTADO=%s\n", m->estado_commited ? "COMMITED" : "WORK_IN_PROGRESS");
    fprintf(f,"BLOCKS=[");
    for (int i=0;i<m->blocks_count;i++){
        fprintf(f,"%d", m->blocks[i]);
        if (i+1 < m->blocks_count) fprintf(f,",");
    }
    fprintf(f,"]\n");
    fclose(f);
    return 0;
}

// add hard link from physical block to logical block
static int add_hardlink_logical_to_physical(const char* file, const char* tag, int logical_idx, int phys_block){
    char src[MAX_PATH], dst[MAX_PATH];
    path_for_blockfile(phys_block, src, sizeof(src));
    path_for_logical_block(file, tag, logical_idx, dst, sizeof(dst));
    // ensure logical_blocks dir
    char lbdir[MAX_PATH];
    snprintf(lbdir, sizeof(lbdir), "%s/%s/%s/logical_blocks", path_files, file, tag);
    mkpath(lbdir);
    // remove if exists
    unlink(dst);
    // create hard link
    if (link(src, dst) == -1) {
        // fallback: copy file
        FILE* fs = fopen(src,"rb");
        if (!fs) return -1;
        FILE* fd = fopen(dst,"wb");
        if (!fd){ fclose(fs); return -1; }
        void* buf = malloc(storage_cfg.block_size);
        fread(buf,1,storage_cfg.block_size,fs);
        fwrite(buf,1,storage_cfg.block_size,fd);
        free(buf); fclose(fs); fclose(fd);
    }
    return 0;
}

// remove logical block file and if physical block no longer referenced, free in bitmap
static void remove_logical_block_and_maybe_free_phys(const char* file, const char* tag, int logical_idx, int phys_block){
    char p[MAX_PATH];
    path_for_logical_block(file, tag, logical_idx, p, sizeof(p));
    unlink(p);
    // determine if phys_block still referenced by any logical block (scan files dir)
    bool still_ref = false;
    DIR* dfiles = opendir(path_files);
    if (dfiles) {
        struct dirent* ent;
        while((ent=readdir(dfiles))){
            if (ent->d_name[0]=='.') continue;
            // for each file
            char pathfile[MAX_PATH];
            snprintf(pathfile,sizeof(pathfile), "%s/%s", path_files, ent->d_name);
            // for each tag dir
            DIR* dtags = opendir(pathfile);
            if (!dtags) continue;
            struct dirent* tent;
            while((tent=readdir(dtags))){
                if (tent->d_name[0]=='.') continue;
                char lb[MAX_PATH];
                snprintf(lb,sizeof(lb), "%s/%s/logical_blocks", pathfile, tent->d_name);
                DIR* dlb = opendir(lb);
                if (!dlb) continue;
                struct dirent* lent;
                while((lent=readdir(dlb))){
                    if (lent->d_name[0]=='.') continue;
                    // check if this hard link points to same inode as physical block
                    char lbpath[MAX_PATH];
                    snprintf(lbpath,sizeof(lbpath), "%s/%s/%s/logical_blocks/%s", path_files, ent->d_name, tent->d_name, lent->d_name);
                    // get inode of lbpath and phys file
                    // simple approach: compare file sizes -> if equals maybe referenced (not robust but ok for base)
                    FILE* f = fopen(lbpath,"rb");
                    if (!f) continue;
                    fseek(f,0,SEEK_END);
                    long s1 = ftell(f);
                    fclose(f);
                    char pphys[MAX_PATH]; path_for_blockfile(phys_block, pphys, sizeof(pphys));
                    FILE* fp = fopen(pphys,"rb");
                    if (!fp) continue;
                    fseek(fp,0,SEEK_END);
                    long s2 = ftell(fp);
                    fclose(fp);
                    if (s1 == s2) { still_ref = true; break; }
                }
                closedir(dlb);
                if (still_ref) break;
            }
            closedir(dtags);
            if (still_ref) break;
        }
        closedir(dfiles);
    }
    if (!still_ref){
        pthread_mutex_lock(&mutex_bitmap);
        set_bit(phys_block, 0);
        pthread_mutex_unlock(&mutex_bitmap);
        printf("## - Bloque Físico Liberado - Número de Bloque: %d\n", phys_block);
        bitmap_save(path_bitmap);
    }
}

// ========== FS initialization ==========
static void storage_init_fresh(){
    // clean folder
    char cmd[2048];
    snprintf(cmd,sizeof(cmd),"rm -rf %s/*", storage_cfg.punto_montaje);
    system(cmd);
    mkpath(storage_cfg.punto_montaje);
    // create files
    FILE* f;
    // superblock
    f = fopen(path_superblock,"w");
    if (!f) { perror("superblock fopen"); exit(EXIT_FAILURE); }
    fprintf(f,"FS_SIZE=%d\nBLOCK_SIZE=%d\n", storage_cfg.fs_size, storage_cfg.block_size);
    fclose(f);
    // bitmap
    bitmap_init_in_memory(storage_cfg.cantidad_bloques);
    // block files dir
    mkpath(path_physical);
    // create physical block files and ensure they exist
    for (int i=0;i<storage_cfg.cantidad_bloques;i++){
        char p[MAX_PATH];
        path_for_blockfile(i,p,sizeof(p));
        FILE* b = fopen(p,"wb");
        if (!b) { perror("block create"); exit(EXIT_FAILURE); }
        void* z = calloc(1, storage_cfg.block_size);
        fwrite(z,1,storage_cfg.block_size,b);
        free(z);
        fclose(b);
    }
    // blocks_hash_index.config empty
    ensure_file_exists(path_hashindex);
    // files dir
    mkpath(path_files);
    // initial_file/BASE with one logical block pointing to block 0
    char filedir[MAX_PATH];
    snprintf(filedir,sizeof(filedir), "%s/initial_file/BASE/logical_blocks", path_files);
    mkpath(filedir);
    // mark block 0 used
    set_bit(0,1);
    bitmap_save(path_bitmap);
    // create hard link logical to block00000
    add_hardlink_logical_to_physical("initial_file","BASE",0,0);
    // metadata
    metadata_t m; m.tam_bytes = storage_cfg.block_size; m.estado_commited = 1;
    m.blocks_count = 1; m.blocks = malloc(sizeof(int)); m.blocks[0]=0;
    write_metadata("initial_file","BASE",&m);
    free(m.blocks);
    printf("Storage formateado con éxito.\n");
}

static void storage_load_existing(){
    // load superblock already read from config; we only need bitmap and directories
    // load bitmap
    FILE* f = fopen(path_bitmap,"rb");
    if (!f) {
        // create default
        bitmap_init_in_memory(storage_cfg.cantidad_bloques);
        bitmap_save(path_bitmap);
        return;
    }
    size_t read = fread(bitmap,1,bitmap_bytes,f);
    if (read != bitmap_bytes) {
        // ok, maybe inconsistent; ignore
    }
    fclose(f);
}

// ========== High-level operations (simplified prototypes) ==========

/*
Protocol messages (from Worker -> Storage), all text, tokens separated by '|':
Examples:
HANDSHAKE_WORKER
GET_BLOCKSIZE
CREATE|<QUERY_ID>|<FILE>|<TAG>
TRUNCATE|<QUERY_ID>|<FILE>|<TAG>|<TAMAÑO_BYTES>
READ|<QUERY_ID>|<FILE>|<TAG>|<BLOQUE_LOGICO>  -> returns content (blocksize)
WRITE|<QUERY_ID>|<FILE>|<TAG>|<BLOQUE_LOGICO>|<BASE64_CONTENT> (we will send raw content after separator)
TAG|<QUERY_ID>|<FILE>|<TAG_ORIGEN>|<TAG_DESTINO>
COMMIT|<QUERY_ID>|<FILE>|<TAG>
DELETE|<QUERY_ID>|<FILE>|<TAG>
*/
static void send_response(int sock, const char* fmt, ...){
    char buf[8192];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    // append newline to mark end
    strncat(buf, "\n", sizeof(buf)-strlen(buf)-1);
    send(sock, buf, strlen(buf), 0);
}

static void op_handshake_worker(int sock){
    // respond with block size
    send_response(sock, "BLOCKSIZE|%d", storage_cfg.block_size);
}

static void op_get_blocksize(int sock){
    send_response(sock, "%d", storage_cfg.block_size);
}

static void op_create(int sock, const char* qid, const char* file, const char* tag){
    metadata_t m; m.tam_bytes = 0; m.estado_commited = 0; m.blocks = NULL; m.blocks_count = 0;
    // create directories
    char dirfile[MAX_PATH]; snprintf(dirfile,sizeof(dirfile), "%s/%s", path_files, file); mkpath(dirfile);
    char dirtag[MAX_PATH]; snprintf(dirtag,sizeof(dirtag), "%s/%s/%s", path_files, file, tag); mkpath(dirtag);
    mkpath(strcat(strcpy(dirfile, dirtag) , "/logical_blocks")); // ensure logical_blocks
    write_metadata(file, tag, &m);
    printf("##%s - File Creado %s:%s\n", qid, file, tag);
    send_response(sock, "OK|CREATE");
}

static void op_truncate(int sock, const char* qid, const char* file, const char* tag, int new_size){
    // read metadata
    metadata_t m;
    if (read_metadata(file, tag, &m) != 0) { send_response(sock,"ERROR|NO_SUCH_TAG"); return; }
    int old_blocks = m.blocks_count;
    int new_blocks = (new_size + storage_cfg.block_size - 1) / storage_cfg.block_size;
    if (new_blocks > old_blocks){
        // append blocks: initially point to physical 0
        int add = new_blocks - old_blocks;
        m.blocks = realloc(m.blocks, sizeof(int)*new_blocks);
        for (int i=0;i<add;i++){
            m.blocks[old_blocks + i] = 0;
            // create hardlink logical to block 0
            add_hardlink_logical_to_physical(file, tag, old_blocks + i, 0);
            printf("##%s - Bloque Físico Reservado - Número de Bloque: %d\n", qid, 0);
            printf("##%s - %s:%s Se agregó el hard link del bloque lógico %d al bloque físico %d\n", qid, file, tag, old_blocks+i, 0);
        }
    } else if (new_blocks < old_blocks){
        // remove last logical blocks
        for (int i = old_blocks - 1; i >= new_blocks; i--){
            int phys = m.blocks[i];
            remove_logical_block_and_maybe_free_phys(file, tag, i, phys);
        }
        m.blocks = realloc(m.blocks, sizeof(int)*new_blocks);
    }
    m.blocks_count = new_blocks;
    m.tam_bytes = new_size;
    write_metadata(file, tag, &m);
    printf("##%s - File Truncado %s:%s - Tamaño: %d\n", qid, file, tag, new_size);
    send_response(sock, "OK|TRUNCATE");
    if (m.blocks) free(m.blocks);
}

static void op_read_block(int sock, const char* qid, const char* file, const char* tag, int logical_block){
    metadata_t m;
    if (read_metadata(file, tag, &m) != 0) { send_response(sock,"ERROR|NO_SUCH_TAG"); return; }
    if (logical_block < 0 || logical_block >= m.blocks_count) { send_response(sock,"ERROR|OUT_OF_RANGE"); return; }
    int phys = m.blocks[logical_block];
    char p[MAX_PATH]; path_for_blockfile(phys,p,sizeof(p));
    usleep(storage_cfg.retardo_operacion * 1000);
    usleep(storage_cfg.retardo_acceso_bloque * 1000);
    FILE* f = fopen(p,"rb");
    if (!f) { send_response(sock,"ERROR|BLOCK_READ_FAIL"); if (m.blocks) free(m.blocks); return; }
    void* buf = malloc(storage_cfg.block_size);
    fread(buf,1,storage_cfg.block_size,f);
    fclose(f);
    // send as binary encoded hex or base64 — for simplicity send raw as hex
    char* md5 = calcular_md5_hex_from_buf(buf, storage_cfg.block_size);
    printf("##%s - Bloque Lógico Leído %s:%s - Número de Bloque: %d\n", qid, file, tag, logical_block);
    // send: OK|READ|<hex-md5>|<blocksize bytes (binary)>
    send(sock, "OK|READ|", strlen("OK|READ|"), 0);
    send(sock, md5, strlen(md5), 0);
    send(sock, "|", 1, 0);
    // send raw block then newline
    send(sock, (const char*)buf, storage_cfg.block_size, 0);
    send(sock, "\n", 1, 0);
    free(md5); free(buf);
    if (m.blocks) free(m.blocks);
}

static void op_write_block(int sock, const char* qid, const char* file, const char* tag, int logical_block, const char* raw_block_buf, size_t buf_len){
    // NOTE: For simplicity this implementation expects the caller to have already uploaded the block content
    metadata_t m;
    if (read_metadata(file, tag, &m) != 0) { send_response(sock,"ERROR|NO_SUCH_TAG"); return; }
    if (logical_block < 0 || logical_block >= m.blocks_count) { send_response(sock,"ERROR|OUT_OF_RANGE"); if (m.blocks) free(m.blocks); return; }
    if (m.estado_commited) { send_response(sock,"ERROR|COMMITED"); if (m.blocks) free(m.blocks); return; }
    int current_phys = m.blocks[logical_block];
    // check if other logical blocks reference same physical (naive scan): if more than 1 reference, need new block
    int refs = 0;
    for (int i=0;i<m.blocks_count;i++) if (m.blocks[i] == current_phys) refs++;
    int target_phys = current_phys;
    if (refs > 1) {
        // allocate new free block
        pthread_mutex_lock(&mutex_bitmap);
        int freeb = find_first_free_block();
        if (freeb == -1) { pthread_mutex_unlock(&mutex_bitmap); send_response(sock,"ERROR|NO_SPACE"); if (m.blocks) free(m.blocks); return; }
        set_bit(freeb,1);
        pthread_mutex_unlock(&mutex_bitmap);
        bitmap_save(path_bitmap);
        ensure_physical_block_exists(freeb, storage_cfg.block_size);
        target_phys = freeb;
        printf("##%s - Bloque Físico Reservado - Número de Bloque: %d\n", qid, target_phys);
    }
    // write raw_block_buf (expected length == block_size) into target_phys file
    char p[MAX_PATH]; path_for_blockfile(target_phys,p,sizeof(p));
    FILE* f = fopen(p,"wb");
    if (!f) { send_response(sock,"ERROR|WRITE_FAIL"); if (m.blocks) free(m.blocks); return; }
    fwrite(raw_block_buf,1,buf_len,f);
    fclose(f);
    // update metadata logical->phys
    m.blocks[logical_block] = target_phys;
    write_metadata(file, tag, &m);
    printf("##%s - Bloque Lógico Escrito %s:%s - Número de Bloque: %d\n", qid, file, tag, logical_block);
    // update hash index: compute md5 of block and if not present add entry
    char* md5 = calcular_md5_hex_from_buf(raw_block_buf, storage_cfg.block_size);
    int found = find_block_by_hash(md5);
    if (found == -1) {
        persist_hash_index_add(md5, target_phys);
    } else if (found != target_phys) {
        // dedup: reassign and free current
        int prev = target_phys;
        m.blocks[logical_block] = found;
        write_metadata(file, tag, &m);
        pthread_mutex_lock(&mutex_bitmap);
        set_bit(prev, 0);
        pthread_mutex_unlock(&mutex_bitmap);
        bitmap_save(path_bitmap);
        printf("##%s - %s:%s Bloque Lógico %d se reasigna de %d a %d\n", qid, file, tag, logical_block, prev, found);
    }
    send_response(sock,"OK|WRITE");
    if (m.blocks) free(m.blocks);
}

static void op_tag(int sock, const char* qid, const char* file, const char* origen, const char* destino){
    // copy directory file/origen to file/destino (deep copy)
    char s_origen[MAX_PATH], s_dest[MAX_PATH];
    snprintf(s_origen,sizeof(s_origen), "%s/%s/%s", path_files, file, origen);
    snprintf(s_dest,sizeof(s_dest), "%s/%s/%s", path_files, file, destino);
    // naive copy using system cp -r
    char cmd[2048];
    snprintf(cmd,sizeof(cmd),"cp -r \"%s\" \"%s\"", s_origen, s_dest);
    system(cmd);
    // set metadata destino to WORK_IN_PROGRESS
    metadata_t m; if (read_metadata(file, destino, &m)==0){ m.estado_commited = 0; write_metadata(file,destino,&m); if (m.blocks) free(m.blocks); }
    printf("##%s - Tag creado %s:%s\n", qid, file, destino);
    send_response(sock,"OK|TAG");
}

static void op_commit(int sock, const char* qid, const char* file, const char* tag){
    metadata_t m;
    if (read_metadata(file, tag, &m) != 0) { send_response(sock,"ERROR|NO_SUCH_TAG"); return; }
    if (m.estado_commited) { send_response(sock,"OK|ALREADY_COMMITED"); if (m.blocks) free(m.blocks); return; }
    // for each block compute md5 and deduplicate if possible
    for (int i=0;i<m.blocks_count;i++){
        char p[MAX_PATH]; path_for_blockfile(m.blocks[i],p,sizeof(p));
        FILE* f = fopen(p,"rb"); if (!f) continue;
        void* buf = malloc(storage_cfg.block_size);
        fread(buf,1,storage_cfg.block_size,f);
        fclose(f);
        char* md5 = calcular_md5_hex_from_buf(buf, storage_cfg.block_size);
        int found = find_block_by_hash(md5);
        if (found == -1){
            persist_hash_index_add(md5, m.blocks[i]);
        } else if (found != m.blocks[i]){
            int prev = m.blocks[i];
            m.blocks[i] = found;
            // remove hard link to prev and maybe free phys
            remove_logical_block_and_maybe_free_phys(file, tag, i, prev);
            printf("##%s - %s:%s Bloque Lógico %d se reasigna de %d a %d\n", qid, file, tag, i, prev, found);
        }
        free(md5); free(buf);
    }
    m.estado_commited = 1;
    write_metadata(file, tag, &m);
    printf("##%s - Commit de File:Tag %s:%s\n", qid, file, tag);
    send_response(sock,"OK|COMMIT");
    if (m.blocks) free(m.blocks);
}

static void op_delete(int sock, const char* qid, const char* file, const char* tag){
    // read metadata to know blocks
    metadata_t m;
    if (read_metadata(file, tag, &m) != 0) { send_response(sock,"ERROR|NO_SUCH_TAG"); return; }
    for (int i=0;i<m.blocks_count;i++){
        int phys = m.blocks[i];
        // remove hardlink and maybe free
        remove_logical_block_and_maybe_free_phys(file, tag, i, phys);
        printf("##%s - %s:%s Se eliminó el hard link del bloque lógico %d al bloque físico %d\n", qid, file, tag, i, phys);
    }
    // remove tag dir
    char tagdir[MAX_PATH]; snprintf(tagdir,sizeof(tagdir), "%s/%s/%s", path_files, file, tag);
    char cmd[2048];
    snprintf(cmd,sizeof(cmd),"rm -rf \"%s\"", tagdir);
    system(cmd);
    printf("##%s - Tag Eliminado %s:%s\n", qid, file, tag);
    send_response(sock,"OK|DELETE");
    if (m.blocks) free(m.blocks);
}

// handle incoming message line and dispatch
static void handle_message(int sock, const char* line){
    // tokenize by '|'
    char *copy = strdup(line);
    trim(copy);
    if (strlen(copy)==0) { free(copy); return; }
    // split
    char* parts[12]; int np = 0;
    char* p = strtok(copy, "|");
    while(p && np < 12){ parts[np++] = p; p = strtok(NULL, "|"); }
    if (np==0) { free(copy); return; }
    if (strcmp(parts[0],"HANDSHAKE_WORKER")==0) { op_handshake_worker(sock); free(copy); return; }
    if (strcmp(parts[0],"GET_BLOCKSIZE")==0) { op_get_blocksize(sock); free(copy); return; }
    if (strcmp(parts[0],"CREATE")==0 && np>=4) { op_create(sock, parts[1], parts[2], parts[3]); free(copy); return; }
    if (strcmp(parts[0],"TRUNCATE")==0 && np>=5) { op_truncate(sock, parts[1], parts[2], parts[3], atoi(parts[4])); free(copy); return; }
    if (strcmp(parts[0],"READ")==0 && np>=5) { op_read_block(sock, parts[1], parts[2], parts[3], atoi(parts[4])); free(copy); return; }
    if (strcmp(parts[0],"WRITE")==0 && np>=6) {
        // WRITE|QID|FILE|TAG|LOGBLOCK|<binary data follows separated by newline> -> here we assume parts[5] is hex or base64; to keep simple, it's transmitted as raw in subsequent recv
        // For simplicity in this base implementation, we expect the caller to send as: WRITE|QID|FILE|TAG|LOGBLOCK|<hex-md5> and then send raw block binary in same socket after newline.
        // We'll attempt to read more data from socket: read block_size bytes
        char qid[64]; strncpy(qid, parts[1], sizeof(qid)-1);
        char file[256]; strncpy(file, parts[2], sizeof(file)-1);
        char tag[256]; strncpy(tag, parts[3], sizeof(tag)-1);
        int logblock = atoi(parts[4]);
        // read block_size bytes
        size_t toread = storage_cfg.block_size;
        void* buf = malloc(toread);
        ssize_t rec = recv(sock, buf, toread, MSG_WAITALL);
        if (rec != (ssize_t)toread){
            // error
            free(buf);
            send_response(sock,"ERROR|WRITE_MISSING_DATA");
        } else {
            // process write
            op_write_block(sock, qid, file, tag, logblock, (const char*)buf, toread);
            free(buf);
        }
        free(copy); return;
    }
    if (strcmp(parts[0],"TAG")==0 && np>=5) { op_tag(sock, parts[1], parts[2], parts[3], parts[4]); free(copy); return; }
    if (strcmp(parts[0],"COMMIT")==0 && np>=4) { op_commit(sock, parts[1], parts[2], parts[3]); free(copy); return; }
    if (strcmp(parts[0],"DELETE")==0 && np>=4) { op_delete(sock, parts[1], parts[2], parts[3]); free(copy); return; }
    // unknown
    send_response(sock,"ERROR|UNKNOWN_CMD");
    free(copy);
}

// ========== Worker thread ==========
static void* atender_worker_thread(void* arg){
    int sock = *(int*)arg;
    free(arg);
    // receive messages line by line
    char buf[MAX_CLIENT_MSG];
    // expect first the worker id or handshake; increment workers count and log
    ssize_t n = recv(sock, buf, sizeof(buf)-1, 0);
    if (n <= 0) { close(sock); return NULL; }
    buf[n]=0;
    trim(buf);
    // Could be "WORKER|<ID>" or "HANDSHAKE_WORKER"
    if (strncmp(buf,"WORKER|",7)==0){
        char* wid = buf + 7;
        pthread_mutex_lock(&mutex_workers_count);
        workers_connected++;
        printf("##Se conecta el Worker %s - Cantidad de Workers: %d\n", wid, workers_connected);
        pthread_mutex_unlock(&mutex_workers_count);
        // continue loop receiving messages
    } else {
        // handle as a command (sometimes handshake)
        handle_message(sock, buf);
    }
    // main command loop
    while(1){
        ssize_t r = recv(sock, buf, sizeof(buf)-1, 0);
        if (r <= 0){
            // disconnect
            // try to parse worker id from previous handshake? For simplicity log generic
            pthread_mutex_lock(&mutex_workers_count);
            if (workers_connected>0) workers_connected--;
            pthread_mutex_unlock(&mutex_workers_count);
            printf("##Se desconecta un Worker - Cantidad de Workers: %d\n", workers_connected);
            close(sock); break;
        }
        buf[r]=0;
        // may contain multiple lines -> process per line
        char* line = strtok(buf, "\n");
        while(line){
            trim(line);
            if (strlen(line)>0) handle_message(sock, line);
            line = strtok(NULL, "\n");
        }
    }
    return NULL;
}

// ========== Server init & main ==========
int main(int argc, char* argv[]){
    if (argc != 2){
        fprintf(stderr, "Uso: %s [archivo_config]\n", argv[0]);
        fprintf(stderr, "Ejemplo: %s storage.config\n", argv[0]);
        return EXIT_FAILURE;
    }
    leer_config_storage(argv[1], &storage_cfg);
    // prepare paths
    snprintf(path_superblock, sizeof(path_superblock), "%s/superblock.config", storage_cfg.punto_montaje);
    snprintf(path_bitmap, sizeof(path_bitmap), "%s/bitmap.bin", storage_cfg.punto_montaje);
    snprintf(path_hashindex, sizeof(path_hashindex), "%s/blocks_hash_index.config", storage_cfg.punto_montaje);
    snprintf(path_physical, sizeof(path_physical), "%s/physical_blocks", storage_cfg.punto_montaje);
    snprintf(path_files, sizeof(path_files), "%s/files", storage_cfg.punto_montaje);

    mkpath(storage_cfg.punto_montaje);
    mkpath(path_physical);
    mkpath(path_files);

    // bitmap in memory
    bitmap_init_in_memory(storage_cfg.cantidad_bloques);

    // init fresh or load
    if (storage_cfg.fresh_start){
        printf("Formateando Storage en %s ...\n", storage_cfg.punto_montaje);
        storage_init_fresh();
    } else {
        // ensure files exist
        ensure_file_exists(path_superblock);
        ensure_file_exists(path_bitmap);
        ensure_file_exists(path_hashindex);
        storage_load_existing();
    }

    // open server socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); return EXIT_FAILURE; }
    int opt = 1; setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(storage_cfg.puerto_escucha);
    if (bind(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return EXIT_FAILURE; }
    if (listen(sockfd, BACKLOG) < 0) { perror("listen"); return EXIT_FAILURE; }
    printf("Storage escuchando en puerto %d ...\n", storage_cfg.puerto_escucha);

    while(1){
        struct sockaddr_in cli;
        socklen_t len = sizeof(cli);
        int client = accept(sockfd, (struct sockaddr*)&cli, &len);
        if (client < 0) { perror("accept"); continue; }
        int* pclient = malloc(sizeof(int));
        *pclient = client;
        pthread_t th;
        pthread_create(&th, NULL, atender_worker_thread, pclient);
        pthread_detach(th);
    }

    return 0;
}
