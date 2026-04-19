#ifndef _NGX_CACHE_PILOT_INDEX_STORE_INTERNAL_H_INCLUDED_
#define _NGX_CACHE_PILOT_INDEX_STORE_INTERNAL_H_INCLUDED_

#include "ngx_cache_pilot_index.h"

#if (NGX_LINUX)

typedef struct ngx_http_cache_index_store_ctx_s
    ngx_http_cache_index_store_ctx_t;
typedef struct ngx_http_cache_index_store_shctx_s
    ngx_http_cache_index_store_shctx_t;
typedef struct ngx_http_cache_index_shm_zone_s
    ngx_http_cache_index_shm_zone_t;
typedef struct ngx_http_cache_index_shm_file_s
    ngx_http_cache_index_shm_file_t;
typedef struct ngx_http_cache_index_shm_key_entry_s
    ngx_http_cache_index_shm_key_entry_t;
typedef struct ngx_http_cache_index_shm_tag_entry_s
    ngx_http_cache_index_shm_tag_entry_t;
typedef struct ngx_http_cache_index_shm_tag_member_s
    ngx_http_cache_index_shm_tag_member_t;

struct ngx_http_cache_index_store_shctx_s {
    ngx_queue_t                    zones;
    ngx_uint_t                     alloc_failures;
};

struct ngx_http_cache_index_store_ctx_s {
    ngx_slab_pool_t               *shpool;
    ngx_http_cache_index_store_shctx_t *sh;
};

struct ngx_http_cache_index_shm_zone_s {
    ngx_queue_t                    queue;
    ngx_queue_t                    files;
    ngx_rbtree_t                   path_index;
    ngx_rbtree_node_t              path_sentinel;
    ngx_rbtree_t                   key_index;
    ngx_rbtree_node_t              key_sentinel;
    ngx_rbtree_t                   tag_index;
    ngx_rbtree_node_t              tag_sentinel;
    ngx_flag_t                     bootstrap_complete;
    time_t                         last_bootstrap_at;
    time_t                         last_updated_at;
    size_t                         name_len;
    u_char                         name[1];
};

struct ngx_http_cache_index_shm_file_s {
    ngx_queue_t                    queue;
    ngx_rbtree_node_t              path_node;
    ngx_queue_t                    key_queue;
    ngx_http_cache_index_shm_key_entry_t *key_entry;
    ngx_queue_t                    tag_members;
    time_t                         mtime;
    off_t                          size;
    ngx_uint_t                     tag_count;
    size_t                         path_len;
    size_t                         key_len;
    u_char                         data[1];
};

struct ngx_http_cache_index_shm_key_entry_s {
    ngx_rbtree_node_t              node;
    ngx_queue_t                    files;
    size_t                         key_len;
    u_char                         key[1];
};

struct ngx_http_cache_index_shm_tag_entry_s {
    ngx_rbtree_node_t              node;
    ngx_queue_t                    files;
    size_t                         tag_len;
    u_char                         tag[1];
};

struct ngx_http_cache_index_shm_tag_member_s {
    ngx_queue_t                    tag_queue;
    ngx_queue_t                    file_queue;
    ngx_http_cache_index_shm_tag_entry_t *tag_entry;
    ngx_http_cache_index_shm_file_t *file;
};

struct ngx_http_cache_index_store_s {
    ngx_http_cache_index_store_ctx_t *ctx;
};

#endif

#endif
