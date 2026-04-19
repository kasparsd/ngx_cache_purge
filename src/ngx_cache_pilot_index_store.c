#include "ngx_cache_pilot_index_store_internal.h"

#if (NGX_LINUX)

static ngx_http_cache_index_store_runtime_t ngx_http_cache_index_store_runtime;

static ngx_int_t ngx_http_cache_index_push_unique(ngx_pool_t *pool,
        ngx_array_t *tags, u_char *data, size_t len);
static ngx_int_t ngx_http_cache_index_path_known(ngx_str_t *path);
static ngx_int_t ngx_http_cache_index_parse_file(ngx_pool_t *pool,
        ngx_str_t *path, ngx_array_t *headers, ngx_array_t **tags,
        ngx_str_t *cache_key_text, time_t *mtime, off_t *size, ngx_log_t *log);
static ngx_int_t ngx_http_cache_index_store_init_zone(ngx_shm_zone_t *shm_zone,
        void *data);
static ngx_http_cache_index_store_t *ngx_http_cache_index_store_shm_open(
    ngx_http_cache_pilot_main_conf_t *pmcf, ngx_log_t *log);
static void ngx_http_cache_index_store_shm_close(
    ngx_http_cache_index_store_t *store);
static ngx_flag_t ngx_http_cache_index_store_shm_lock(
    ngx_http_cache_index_store_t *store);
static void ngx_http_cache_index_store_shm_unlock(
    ngx_http_cache_index_store_t *store, ngx_flag_t locked);
static ngx_int_t ngx_http_cache_index_store_shm_upsert_file_meta(
    ngx_http_cache_index_store_t *store, ngx_str_t *zone_name,
    ngx_str_t *path, ngx_str_t *cache_key_text, time_t mtime, off_t size,
    ngx_array_t *tags, ngx_log_t *log);
static ngx_int_t ngx_http_cache_index_store_shm_delete_file(
    ngx_http_cache_index_store_t *store, ngx_str_t *zone_name,
    ngx_str_t *path, ngx_log_t *log);
static ngx_int_t ngx_http_cache_index_store_shm_collect_paths_by_tags(
    ngx_http_cache_index_store_t *store, ngx_pool_t *pool,
    ngx_str_t *zone_name, ngx_array_t *tags, ngx_array_t **paths,
    ngx_log_t *log);
static ngx_int_t ngx_http_cache_index_store_shm_collect_paths_by_exact_key(
    ngx_http_cache_index_store_t *store, ngx_pool_t *pool,
    ngx_str_t *zone_name, ngx_str_t *key_text, ngx_array_t **paths,
    ngx_log_t *log);
static ngx_int_t ngx_http_cache_index_store_shm_collect_paths_by_key_prefix(
    ngx_http_cache_index_store_t *store, ngx_pool_t *pool,
    ngx_str_t *zone_name, ngx_str_t *prefix, ngx_array_t **paths,
    ngx_log_t *log);
static ngx_int_t ngx_http_cache_index_store_shm_get_zone_state(
    ngx_http_cache_index_store_t *store, ngx_str_t *zone_name,
    ngx_http_cache_index_zone_state_t *state, ngx_log_t *log);
static ngx_int_t ngx_http_cache_index_store_shm_set_zone_state(
    ngx_http_cache_index_store_t *store, ngx_str_t *zone_name,
    ngx_http_cache_index_zone_state_t *state, ngx_log_t *log);
typedef ngx_str_t (*ngx_http_cache_index_store_shm_node_string_pt)(
    ngx_rbtree_node_t *node);
typedef void *(*ngx_http_cache_index_store_shm_node_data_pt)(
    ngx_rbtree_node_t *node);
typedef ngx_int_t (*ngx_http_cache_index_store_shm_node_cmp_pt)(
    ngx_str_t *needle, void *entry);
static void ngx_http_cache_index_store_shm_insert_generic(
    ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node,
    ngx_rbtree_node_t *sentinel,
    ngx_http_cache_index_store_shm_node_string_pt node_string,
    ngx_http_cache_index_store_shm_node_data_pt node_data,
    ngx_http_cache_index_store_shm_node_cmp_pt node_cmp);
static void *ngx_http_cache_index_store_shm_lookup_generic(
    ngx_rbtree_t *tree, uint32_t hash, ngx_str_t *needle,
    ngx_http_cache_index_store_shm_node_data_pt node_data,
    ngx_http_cache_index_store_shm_node_cmp_pt node_cmp);
static ngx_str_t ngx_http_cache_index_store_shm_path_node_string(
    ngx_rbtree_node_t *node);
static ngx_str_t ngx_http_cache_index_store_shm_key_node_string(
    ngx_rbtree_node_t *node);
static ngx_str_t ngx_http_cache_index_store_shm_tag_node_string(
    ngx_rbtree_node_t *node);
static void *ngx_http_cache_index_store_shm_path_node_data(
    ngx_rbtree_node_t *node);
static void *ngx_http_cache_index_store_shm_key_node_data(
    ngx_rbtree_node_t *node);
static void *ngx_http_cache_index_store_shm_tag_node_data(
    ngx_rbtree_node_t *node);
static ngx_int_t ngx_http_cache_index_store_shm_path_node_cmp(
    ngx_str_t *needle, void *entry);
static ngx_int_t ngx_http_cache_index_store_shm_key_node_cmp(
    ngx_str_t *needle, void *entry);
static ngx_int_t ngx_http_cache_index_store_shm_tag_node_cmp(
    ngx_str_t *needle, void *entry);
static void ngx_http_cache_index_store_shm_path_insert(
    ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node,
    ngx_rbtree_node_t *sentinel);
static void ngx_http_cache_index_store_shm_key_insert(
    ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node,
    ngx_rbtree_node_t *sentinel);
static void ngx_http_cache_index_store_shm_tag_insert(
    ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node,
    ngx_rbtree_node_t *sentinel);
static ngx_http_cache_index_shm_zone_t *ngx_http_cache_index_store_shm_lookup_zone(
    ngx_http_cache_index_store_shctx_t *sh, ngx_str_t *zone_name);
static ngx_http_cache_index_shm_zone_t *ngx_http_cache_index_store_shm_get_zone_locked(
    ngx_http_cache_index_store_ctx_t *ctx, ngx_str_t *zone_name,
    ngx_flag_t create);
static ngx_http_cache_index_shm_key_entry_t *
ngx_http_cache_index_store_shm_lookup_key_entry(
    ngx_http_cache_index_shm_zone_t *zone, ngx_str_t *key);
static ngx_http_cache_index_shm_key_entry_t *
ngx_http_cache_index_store_shm_get_key_entry_locked(
    ngx_http_cache_index_store_ctx_t *ctx, ngx_http_cache_index_shm_zone_t *zone,
    ngx_str_t *key, ngx_flag_t create);
static ngx_http_cache_index_shm_tag_entry_t *
ngx_http_cache_index_store_shm_lookup_tag_entry(
    ngx_http_cache_index_shm_zone_t *zone, ngx_str_t *tag);
static ngx_http_cache_index_shm_tag_entry_t *
ngx_http_cache_index_store_shm_get_tag_entry_locked(
    ngx_http_cache_index_store_ctx_t *ctx, ngx_http_cache_index_shm_zone_t *zone,
    ngx_str_t *tag, ngx_flag_t create);
static ngx_http_cache_index_shm_file_t *ngx_http_cache_index_store_shm_lookup_file(
    ngx_http_cache_index_shm_zone_t *zone, ngx_str_t *path);
static ngx_int_t ngx_http_cache_index_store_shm_remove_file_locked(
    ngx_http_cache_index_store_ctx_t *ctx, ngx_http_cache_index_shm_zone_t *zone,
    ngx_http_cache_index_shm_file_t *file);
static ngx_int_t ngx_http_cache_index_store_shm_push_path_unique(
    ngx_pool_t *pool, ngx_array_t *paths, u_char *data, size_t len);
static void *ngx_http_cache_index_store_shm_alloc_locked(
    ngx_http_cache_index_store_ctx_t *ctx, size_t size);
static u_char *ngx_http_cache_index_store_shm_file_path(
    ngx_http_cache_index_shm_file_t *file);
static u_char *ngx_http_cache_index_store_shm_file_key(
    ngx_http_cache_index_shm_file_t *file);
static void ngx_http_cache_index_store_shm_touch_zone_locked(
    ngx_http_cache_index_shm_zone_t *zone);
static ngx_int_t ngx_http_cache_index_store_shm_path_cmp(
    ngx_str_t *path, ngx_http_cache_index_shm_file_t *file);
static ngx_int_t ngx_http_cache_index_store_shm_key_cmp(
    ngx_str_t *key, ngx_http_cache_index_shm_key_entry_t *entry);
static ngx_int_t ngx_http_cache_index_store_shm_tag_cmp(
    ngx_str_t *tag, ngx_http_cache_index_shm_tag_entry_t *entry);
static ngx_int_t ngx_http_cache_index_store_shm_link_exact_key_locked(
    ngx_http_cache_index_store_ctx_t *ctx, ngx_http_cache_index_shm_zone_t *zone,
    ngx_http_cache_index_shm_file_t *file);
static ngx_int_t ngx_http_cache_index_store_shm_link_tags_locked(
    ngx_http_cache_index_store_ctx_t *ctx, ngx_http_cache_index_shm_zone_t *zone,
    ngx_http_cache_index_shm_file_t *file, ngx_array_t *tags);
static void ngx_http_cache_index_store_shm_free_empty_key_entry_locked(
    ngx_http_cache_index_store_ctx_t *ctx, ngx_http_cache_index_shm_zone_t *zone,
    ngx_http_cache_index_shm_key_entry_t *entry);
static void ngx_http_cache_index_store_shm_free_empty_tag_entry_locked(
    ngx_http_cache_index_store_ctx_t *ctx, ngx_http_cache_index_shm_zone_t *zone,
    ngx_http_cache_index_shm_tag_entry_t *entry);

ngx_int_t
ngx_http_cache_index_store_init_conf(ngx_conf_t *cf,
                                     ngx_http_cache_pilot_main_conf_t *pmcf) {
    ngx_http_cache_index_store_ctx_t  *ctx;
    ngx_str_t                          zone_name = ngx_string("cache_pilot_index");

    if (pmcf == NULL) {
        return NGX_OK;
    }

    ctx = ngx_pcalloc(cf->pool, sizeof(ngx_http_cache_index_store_ctx_t));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    pmcf->index_zone = ngx_shared_memory_add(cf, &zone_name,
                       pmcf->index_shm_size,
                       &ngx_http_cache_pilot_module);
    if (pmcf->index_zone == NULL) {
        return NGX_ERROR;
    }

    pmcf->index_zone->init = ngx_http_cache_index_store_init_zone;
    pmcf->index_zone->data = ctx;

    return NGX_OK;
}

ngx_flag_t
ngx_http_cache_index_store_configured(ngx_http_cache_pilot_main_conf_t *pmcf) {
    return pmcf != NULL && pmcf->index_zone != NULL;
}

ngx_http_cache_index_store_t *
ngx_http_cache_index_store_open_writer(ngx_http_cache_pilot_main_conf_t *pmcf,
                                       ngx_log_t *log) {
    if (!ngx_http_cache_index_store_configured(pmcf)) {
        return NULL;
    }

    return ngx_http_cache_index_store_shm_open(pmcf, log);
}

ngx_http_cache_index_store_t *
ngx_http_cache_index_store_open_reader(ngx_http_cache_pilot_main_conf_t *pmcf,
                                       ngx_log_t *log) {
    if (!ngx_http_cache_index_store_configured(pmcf)) {
        return NULL;
    }

    return ngx_http_cache_index_store_shm_open(pmcf, log);
}

void
ngx_http_cache_index_store_close(ngx_http_cache_index_store_t *store) {
    if (store == NULL) {
        return;
    }

    /* Close the backing resource (socket / db handle).  The store struct
     * itself is allocated from cycle->pool and freed automatically when
     * the pool is destroyed on worker exit or config reload. */
    ngx_http_cache_index_store_shm_close(store);
}

ngx_int_t
ngx_http_cache_index_store_upsert_file_meta(ngx_http_cache_index_store_t *store,
        ngx_str_t *zone_name, ngx_str_t *path, ngx_str_t *cache_key_text,
        time_t mtime, off_t size, ngx_array_t *tags, ngx_log_t *log) {
    return ngx_http_cache_index_store_shm_upsert_file_meta(store, zone_name, path,
            cache_key_text, mtime, size, tags, log);
}

ngx_int_t
ngx_http_cache_index_store_collect_paths_by_exact_key(
    ngx_http_cache_index_store_t *store, ngx_pool_t *pool,
    ngx_str_t *zone_name, ngx_str_t *key_text,
    ngx_array_t **paths, ngx_log_t *log) {
    return ngx_http_cache_index_store_shm_collect_paths_by_exact_key(store, pool,
            zone_name, key_text, paths, log);
}

ngx_int_t
ngx_http_cache_index_store_collect_paths_by_key_prefix(
    ngx_http_cache_index_store_t *store, ngx_pool_t *pool,
    ngx_str_t *zone_name, ngx_str_t *prefix,
    ngx_array_t **paths, ngx_log_t *log) {
    return ngx_http_cache_index_store_shm_collect_paths_by_key_prefix(store, pool,
            zone_name, prefix, paths, log);
}

ngx_int_t
ngx_http_cache_index_store_delete_file(ngx_http_cache_index_store_t *store,
                                       ngx_str_t *zone_name, ngx_str_t *path,
                                       ngx_log_t *log) {
    return ngx_http_cache_index_store_shm_delete_file(store, zone_name, path, log);
}

ngx_int_t
ngx_http_cache_index_store_collect_paths_by_tags(ngx_http_cache_index_store_t *store,
        ngx_pool_t *pool, ngx_str_t *zone_name, ngx_array_t *tags,
        ngx_array_t **paths, ngx_log_t *log) {
    return ngx_http_cache_index_store_shm_collect_paths_by_tags(store, pool,
            zone_name, tags, paths, log);
}

ngx_int_t
ngx_http_cache_index_store_get_zone_state(ngx_http_cache_index_store_t *store,
        ngx_str_t *zone_name,
        ngx_http_cache_index_zone_state_t *state,
        ngx_log_t *log) {
    return ngx_http_cache_index_store_shm_get_zone_state(store, zone_name, state,
            log);
}

ngx_int_t
ngx_http_cache_index_store_set_zone_state(ngx_http_cache_index_store_t *store,
        ngx_str_t *zone_name,
        ngx_http_cache_index_zone_state_t *state,
        ngx_log_t *log) {
    return ngx_http_cache_index_store_shm_set_zone_state(store, zone_name, state,
            log);
}

ngx_int_t
ngx_http_cache_index_store_process_file(ngx_http_cache_index_store_t *store,
                                        ngx_str_t *zone_name, ngx_str_t *path,
                                        ngx_array_t *headers,
                                        ngx_log_t *log) {
    ngx_pool_t   *pool;
    ngx_array_t  *tags;
    ngx_str_t     cache_key_text;
    time_t        mtime;
    off_t         size;
    ngx_int_t     parse_rc;
    ngx_int_t     rc;

    pool = ngx_create_pool(4096, log);
    if (pool == NULL) {
        return NGX_ERROR;
    }

    ngx_str_null(&cache_key_text);

    if (headers == NULL || headers->nelts == 0) {
        /* No configured tag headers: still parse for KEY: so the key index
         * is populated even when tag indexing is not in use. */
        parse_rc = ngx_http_cache_index_parse_file(pool, path, NULL, &tags,
                   &cache_key_text, &mtime, &size,
                   log);
        if (parse_rc != NGX_OK) {
            ngx_log_debug3(NGX_LOG_DEBUG_HTTP, log, 0,
                           "cache_tag process_file skipped zone:\"%V\" path:\"%V\" rc:%i",
                           zone_name, path, parse_rc);
            ngx_destroy_pool(pool);
            return ngx_http_cache_index_store_delete_file(store, zone_name, path,
                    log);
        }

        ngx_log_debug5(NGX_LOG_DEBUG_HTTP, log, 0,
                       "cache_tag process_file zone:\"%V\" path:\"%V\" key_len:%uz tags:%ui headers:%ui",
                       zone_name, path, cache_key_text.len,
                       tags != NULL ? tags->nelts : 0,
                       headers != NULL ? headers->nelts : 0);

        rc = ngx_http_cache_index_store_upsert_file_meta(store, zone_name, path,
                &cache_key_text, mtime, size, NULL, log);
        ngx_destroy_pool(pool);
        return rc;
    }

    parse_rc = ngx_http_cache_index_parse_file(pool, path, headers, &tags,
               &cache_key_text, &mtime, &size,
               log);
    if (parse_rc != NGX_OK) {
        ngx_log_debug3(NGX_LOG_DEBUG_HTTP, log, 0,
                       "cache_tag process_file skipped zone:\"%V\" path:\"%V\" rc:%i",
                       zone_name, path, parse_rc);
        ngx_destroy_pool(pool);
        return ngx_http_cache_index_store_delete_file(store, zone_name, path, log);
    }

    ngx_log_debug5(NGX_LOG_DEBUG_HTTP, log, 0,
                   "cache_tag process_file zone:\"%V\" path:\"%V\" key_len:%uz tags:%ui headers:%ui",
                   zone_name, path, cache_key_text.len,
                   tags != NULL ? tags->nelts : 0,
                   headers != NULL ? headers->nelts : 0);

    rc = ngx_http_cache_index_store_upsert_file_meta(store, zone_name, path,
            &cache_key_text, mtime, size, tags, log);
    ngx_destroy_pool(pool);

    return rc;
}

ngx_int_t
ngx_http_cache_index_store_runtime_init(ngx_cycle_t *cycle,
                                        ngx_http_cache_pilot_main_conf_t *pmcf,
                                        ngx_flag_t owner) {
    ngx_memzero(&ngx_http_cache_index_store_runtime,
                sizeof(ngx_http_cache_index_store_runtime));

    ngx_http_cache_index_store_runtime.cycle = cycle;
    ngx_http_cache_index_store_runtime.owner = owner;

    if (!ngx_http_cache_index_store_configured(pmcf)) {
        return NGX_OK;
    }

    ngx_http_cache_index_store_runtime.reader =
        ngx_http_cache_index_store_open_reader(pmcf, cycle->log);
    if (ngx_http_cache_index_store_runtime.reader == NULL) {
        return NGX_ERROR;
    }

    if (owner) {
        ngx_http_cache_index_store_runtime.writer =
            ngx_http_cache_index_store_runtime.reader;
    }

    return NGX_OK;
}

void
ngx_http_cache_index_store_runtime_shutdown(void) {
    ngx_http_cache_index_store_close(ngx_http_cache_index_store_runtime.writer);
    ngx_http_cache_index_store_close(ngx_http_cache_index_store_runtime.reader);

    ngx_memzero(&ngx_http_cache_index_store_runtime,
                sizeof(ngx_http_cache_index_store_runtime));
}

ngx_http_cache_index_store_t *
ngx_http_cache_index_store_writer(void) {
    return ngx_http_cache_index_store_runtime.writer;
}

ngx_http_cache_index_store_t *
ngx_http_cache_index_store_reader(ngx_http_cache_pilot_main_conf_t *pmcf,
                                  ngx_log_t *log) {
    if (ngx_http_cache_index_store_runtime.reader != NULL) {
        return ngx_http_cache_index_store_runtime.reader;
    }

    ngx_http_cache_index_store_runtime.reader =
        ngx_http_cache_index_store_open_reader(pmcf, log);

    return ngx_http_cache_index_store_runtime.reader;
}

static ngx_int_t
ngx_http_cache_index_store_init_zone(ngx_shm_zone_t *shm_zone, void *data) {
    ngx_http_cache_index_store_ctx_t    *octx = data;
    ngx_http_cache_index_store_ctx_t    *ctx;
    ngx_http_cache_index_store_shctx_t  *sh;

    ctx = shm_zone->data;

    if (octx != NULL) {
        ctx->shpool = octx->shpool;
        ctx->sh = octx->sh;
        return NGX_OK;
    }

    ctx->shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;
    if (shm_zone->shm.exists) {
        ctx->sh = ctx->shpool->data;
        return NGX_OK;
    }

    sh = ngx_slab_calloc(ctx->shpool, sizeof(ngx_http_cache_index_store_shctx_t));
    if (sh == NULL) {
        return NGX_ERROR;
    }

    ngx_queue_init(&sh->zones);

    ctx->shpool->data = sh;
    ctx->sh = sh;

    return NGX_OK;
}

static ngx_http_cache_index_store_t *
ngx_http_cache_index_store_shm_open(ngx_http_cache_pilot_main_conf_t *pmcf,
                                    ngx_log_t *log) {
    ngx_http_cache_index_store_t      *store;
    ngx_http_cache_index_store_ctx_t  *ctx;

    (void) log;

    if (pmcf == NULL || pmcf->index_zone == NULL || pmcf->index_zone->data == NULL) {
        return NULL;
    }

    ctx = pmcf->index_zone->data;
    if (ctx->shpool == NULL || ctx->sh == NULL) {
        return NULL;
    }

    store = ngx_pcalloc(ngx_cycle->pool, sizeof(ngx_http_cache_index_store_t));
    if (store == NULL) {
        return NULL;
    }

    store->ctx = ctx;

    return store;
}

static void
ngx_http_cache_index_store_shm_close(ngx_http_cache_index_store_t *store) {
    (void) store;
}

static ngx_flag_t
ngx_http_cache_index_store_shm_lock(ngx_http_cache_index_store_t *store) {
    ngx_shmtx_lock(&store->ctx->shpool->mutex);

    return 1;
}

static void
ngx_http_cache_index_store_shm_unlock(ngx_http_cache_index_store_t *store,
                                      ngx_flag_t locked) {
    if (locked) {
        ngx_shmtx_unlock(&store->ctx->shpool->mutex);
    }
}

static void
ngx_http_cache_index_store_shm_insert_generic(ngx_rbtree_node_t *temp,
        ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel,
        ngx_http_cache_index_store_shm_node_string_pt node_string,
        ngx_http_cache_index_store_shm_node_data_pt node_data,
        ngx_http_cache_index_store_shm_node_cmp_pt node_cmp) {
    ngx_str_t   needle;
    ngx_int_t   rc;

    needle = node_string(node);

    for (;;) {
        if (node->key < temp->key) {
            if (temp->left == sentinel) {
                temp->left = node;
                break;
            }

            temp = temp->left;
            continue;
        }

        if (node->key > temp->key) {
            if (temp->right == sentinel) {
                temp->right = node;
                break;
            }

            temp = temp->right;
            continue;
        }

        rc = node_cmp(&needle, node_data(temp));
        if (rc < 0) {
            if (temp->left == sentinel) {
                temp->left = node;
                break;
            }

            temp = temp->left;
            continue;
        }

        if (temp->right == sentinel) {
            temp->right = node;
            break;
        }

        temp = temp->right;
    }

    node->parent = temp;
    node->left = sentinel;
    node->right = sentinel;
    ngx_rbt_red(node);
}

static ngx_str_t
ngx_http_cache_index_store_shm_path_node_string(ngx_rbtree_node_t *node) {
    ngx_http_cache_index_shm_file_t  *file;
    ngx_str_t                         path;

    file = ngx_rbtree_data(node, ngx_http_cache_index_shm_file_t, path_node);
    path.len = file->path_len;
    path.data = ngx_http_cache_index_store_shm_file_path(file);

    return path;
}

static ngx_str_t
ngx_http_cache_index_store_shm_key_node_string(ngx_rbtree_node_t *node) {
    ngx_http_cache_index_shm_key_entry_t  *entry;
    ngx_str_t                              key;

    entry = ngx_rbtree_data(node, ngx_http_cache_index_shm_key_entry_t, node);
    key.len = entry->key_len;
    key.data = entry->key;

    return key;
}

static ngx_str_t
ngx_http_cache_index_store_shm_tag_node_string(ngx_rbtree_node_t *node) {
    ngx_http_cache_index_shm_tag_entry_t  *entry;
    ngx_str_t                              tag;

    entry = ngx_rbtree_data(node, ngx_http_cache_index_shm_tag_entry_t, node);
    tag.len = entry->tag_len;
    tag.data = entry->tag;

    return tag;
}

static void *
ngx_http_cache_index_store_shm_path_node_data(ngx_rbtree_node_t *node) {
    return ngx_rbtree_data(node, ngx_http_cache_index_shm_file_t, path_node);
}

static void *
ngx_http_cache_index_store_shm_key_node_data(ngx_rbtree_node_t *node) {
    return ngx_rbtree_data(node, ngx_http_cache_index_shm_key_entry_t, node);
}

static void *
ngx_http_cache_index_store_shm_tag_node_data(ngx_rbtree_node_t *node) {
    return ngx_rbtree_data(node, ngx_http_cache_index_shm_tag_entry_t, node);
}

static ngx_int_t
ngx_http_cache_index_store_shm_path_node_cmp(ngx_str_t *needle, void *entry) {
    return ngx_http_cache_index_store_shm_path_cmp(needle, entry);
}

static ngx_int_t
ngx_http_cache_index_store_shm_key_node_cmp(ngx_str_t *needle, void *entry) {
    return ngx_http_cache_index_store_shm_key_cmp(needle, entry);
}

static ngx_int_t
ngx_http_cache_index_store_shm_tag_node_cmp(ngx_str_t *needle, void *entry) {
    return ngx_http_cache_index_store_shm_tag_cmp(needle, entry);
}

static void
ngx_http_cache_index_store_shm_path_insert(ngx_rbtree_node_t *temp,
        ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel) {
    ngx_http_cache_index_store_shm_insert_generic(
        temp, node, sentinel,
        ngx_http_cache_index_store_shm_path_node_string,
        ngx_http_cache_index_store_shm_path_node_data,
        ngx_http_cache_index_store_shm_path_node_cmp);
}

static void
ngx_http_cache_index_store_shm_key_insert(ngx_rbtree_node_t *temp,
        ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel) {
    ngx_http_cache_index_store_shm_insert_generic(
        temp, node, sentinel,
        ngx_http_cache_index_store_shm_key_node_string,
        ngx_http_cache_index_store_shm_key_node_data,
        ngx_http_cache_index_store_shm_key_node_cmp);
}

static void
ngx_http_cache_index_store_shm_tag_insert(ngx_rbtree_node_t *temp,
        ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel) {
    ngx_http_cache_index_store_shm_insert_generic(
        temp, node, sentinel,
        ngx_http_cache_index_store_shm_tag_node_string,
        ngx_http_cache_index_store_shm_tag_node_data,
        ngx_http_cache_index_store_shm_tag_node_cmp);
}

static ngx_http_cache_index_shm_zone_t *
ngx_http_cache_index_store_shm_lookup_zone(ngx_http_cache_index_store_shctx_t *sh,
        ngx_str_t *zone_name) {
    ngx_queue_t                     *q;
    ngx_http_cache_index_shm_zone_t *zone;

    for (q = ngx_queue_head(&sh->zones);
            q != ngx_queue_sentinel(&sh->zones);
            q = ngx_queue_next(q)) {
        zone = ngx_queue_data(q, ngx_http_cache_index_shm_zone_t, queue);
        if (zone->name_len == zone_name->len
                && ngx_strncmp(zone->name, zone_name->data, zone_name->len) == 0) {
            return zone;
        }
    }

    return NULL;
}

static void *
ngx_http_cache_index_store_shm_alloc_locked(ngx_http_cache_index_store_ctx_t *ctx,
        size_t size) {
    void *p;

    p = ngx_slab_alloc_locked(ctx->shpool, size);
    if (p == NULL && ctx->sh != NULL) {
        ctx->sh->alloc_failures++;
    }

    return p;
}

static ngx_http_cache_index_shm_zone_t *
ngx_http_cache_index_store_shm_get_zone_locked(ngx_http_cache_index_store_ctx_t *ctx,
        ngx_str_t *zone_name,
        ngx_flag_t create) {
    ngx_http_cache_index_shm_zone_t *zone;
    size_t                           size;

    zone = ngx_http_cache_index_store_shm_lookup_zone(ctx->sh, zone_name);
    if (zone != NULL || !create) {
        return zone;
    }

    size = sizeof(ngx_http_cache_index_shm_zone_t) + zone_name->len;
    zone = ngx_http_cache_index_store_shm_alloc_locked(ctx, size);
    if (zone == NULL) {
        return NULL;
    }

    ngx_memzero(zone, sizeof(ngx_http_cache_index_shm_zone_t));
    zone->name_len = zone_name->len;
    ngx_memcpy(zone->name, zone_name->data, zone_name->len);
    zone->name[zone_name->len] = '\0';
    ngx_queue_init(&zone->files);
    ngx_rbtree_init(&zone->path_index, &zone->path_sentinel,
                    ngx_http_cache_index_store_shm_path_insert);
    ngx_rbtree_init(&zone->key_index, &zone->key_sentinel,
                    ngx_http_cache_index_store_shm_key_insert);
    ngx_rbtree_init(&zone->tag_index, &zone->tag_sentinel,
                    ngx_http_cache_index_store_shm_tag_insert);
    ngx_queue_insert_tail(&ctx->sh->zones, &zone->queue);

    return zone;
}

static void *
ngx_http_cache_index_store_shm_lookup_generic(ngx_rbtree_t *tree,
        uint32_t hash, ngx_str_t *needle,
        ngx_http_cache_index_store_shm_node_data_pt node_data,
        ngx_http_cache_index_store_shm_node_cmp_pt node_cmp) {
    ngx_rbtree_node_t  *node, *sentinel;
    ngx_int_t           rc;

    node = tree->root;
    sentinel = tree->sentinel;

    while (node != sentinel) {
        if (hash < node->key) {
            node = node->left;
            continue;
        }

        if (hash > node->key) {
            node = node->right;
            continue;
        }

        rc = node_cmp(needle, node_data(node));
        if (rc == 0) {
            return node_data(node);
        }

        node = rc < 0 ? node->left : node->right;
    }

    return NULL;
}

static ngx_http_cache_index_shm_key_entry_t *
ngx_http_cache_index_store_shm_lookup_key_entry(
    ngx_http_cache_index_shm_zone_t *zone, ngx_str_t *key) {
    uint32_t                             hash;

    hash = ngx_crc32_short(key->data, key->len);

    return ngx_http_cache_index_store_shm_lookup_generic(
               &zone->key_index, hash, key,
               ngx_http_cache_index_store_shm_key_node_data,
               ngx_http_cache_index_store_shm_key_node_cmp);
}

static ngx_http_cache_index_shm_key_entry_t *
ngx_http_cache_index_store_shm_get_key_entry_locked(
    ngx_http_cache_index_store_ctx_t *ctx,
    ngx_http_cache_index_shm_zone_t *zone, ngx_str_t *key,
    ngx_flag_t create) {
    ngx_http_cache_index_shm_key_entry_t *entry;
    size_t                                size;

    entry = ngx_http_cache_index_store_shm_lookup_key_entry(zone, key);
    if (entry != NULL || !create) {
        return entry;
    }

    size = sizeof(ngx_http_cache_index_shm_key_entry_t) + key->len;
    entry = ngx_http_cache_index_store_shm_alloc_locked(ctx, size);
    if (entry == NULL) {
        return NULL;
    }

    ngx_memzero(entry, sizeof(ngx_http_cache_index_shm_key_entry_t));
    ngx_queue_init(&entry->files);
    entry->key_len = key->len;
    ngx_memcpy(entry->key, key->data, key->len);
    entry->key[key->len] = '\0';
    entry->node.key = ngx_crc32_short(key->data, key->len);
    ngx_rbtree_insert(&zone->key_index, &entry->node);

    return entry;
}

static ngx_http_cache_index_shm_tag_entry_t *
ngx_http_cache_index_store_shm_lookup_tag_entry(
    ngx_http_cache_index_shm_zone_t *zone, ngx_str_t *tag) {
    uint32_t                             hash;

    hash = ngx_crc32_short(tag->data, tag->len);

    return ngx_http_cache_index_store_shm_lookup_generic(
               &zone->tag_index, hash, tag,
               ngx_http_cache_index_store_shm_tag_node_data,
               ngx_http_cache_index_store_shm_tag_node_cmp);
}

static ngx_http_cache_index_shm_tag_entry_t *
ngx_http_cache_index_store_shm_get_tag_entry_locked(
    ngx_http_cache_index_store_ctx_t *ctx,
    ngx_http_cache_index_shm_zone_t *zone, ngx_str_t *tag,
    ngx_flag_t create) {
    ngx_http_cache_index_shm_tag_entry_t *entry;
    size_t                                size;

    entry = ngx_http_cache_index_store_shm_lookup_tag_entry(zone, tag);
    if (entry != NULL || !create) {
        return entry;
    }

    size = sizeof(ngx_http_cache_index_shm_tag_entry_t) + tag->len;
    entry = ngx_http_cache_index_store_shm_alloc_locked(ctx, size);
    if (entry == NULL) {
        return NULL;
    }

    ngx_memzero(entry, sizeof(ngx_http_cache_index_shm_tag_entry_t));
    ngx_queue_init(&entry->files);
    entry->tag_len = tag->len;
    ngx_memcpy(entry->tag, tag->data, tag->len);
    entry->tag[tag->len] = '\0';
    entry->node.key = ngx_crc32_short(tag->data, tag->len);
    ngx_rbtree_insert(&zone->tag_index, &entry->node);

    return entry;
}

static ngx_http_cache_index_shm_file_t *
ngx_http_cache_index_store_shm_lookup_file(ngx_http_cache_index_shm_zone_t *zone,
        ngx_str_t *path) {
    uint32_t                        hash;

    hash = ngx_crc32_short(path->data, path->len);

    return ngx_http_cache_index_store_shm_lookup_generic(
               &zone->path_index, hash, path,
               ngx_http_cache_index_store_shm_path_node_data,
               ngx_http_cache_index_store_shm_path_node_cmp);
}

static ngx_int_t
ngx_http_cache_index_store_shm_remove_file_locked(ngx_http_cache_index_store_ctx_t *ctx,
        ngx_http_cache_index_shm_zone_t *zone,
        ngx_http_cache_index_shm_file_t *file) {
    ngx_queue_t                        *q;
    ngx_queue_t                        *next;
    ngx_http_cache_index_shm_tag_member_t *member;

    if (file->key_entry != NULL) {
        ngx_queue_remove(&file->key_queue);
        ngx_http_cache_index_store_shm_free_empty_key_entry_locked(
            ctx, zone, file->key_entry);
        file->key_entry = NULL;
    }

    for (q = ngx_queue_head(&file->tag_members);
            q != ngx_queue_sentinel(&file->tag_members);
            q = next) {
        next = ngx_queue_next(q);
        member = ngx_queue_data(q, ngx_http_cache_index_shm_tag_member_t,
                                file_queue);
        ngx_queue_remove(&member->tag_queue);
        ngx_queue_remove(&member->file_queue);
        ngx_http_cache_index_store_shm_free_empty_tag_entry_locked(
            ctx, zone, member->tag_entry);
        ngx_slab_free_locked(ctx->shpool, member);
    }

    ngx_rbtree_delete(&zone->path_index, &file->path_node);
    ngx_queue_remove(&file->queue);
    ngx_slab_free_locked(ctx->shpool, file);

    return NGX_OK;
}

static u_char *
ngx_http_cache_index_store_shm_file_path(ngx_http_cache_index_shm_file_t *file) {
    return file->data;
}

static u_char *
ngx_http_cache_index_store_shm_file_key(ngx_http_cache_index_shm_file_t *file) {
    return file->data + file->path_len + 1;
}

static void
ngx_http_cache_index_store_shm_touch_zone_locked(
    ngx_http_cache_index_shm_zone_t *zone) {
    zone->last_updated_at = ngx_time();
}

static ngx_int_t
ngx_http_cache_index_store_shm_path_cmp(ngx_str_t *path,
                                        ngx_http_cache_index_shm_file_t *file) {
    return ngx_memn2cmp(path->data,
                        ngx_http_cache_index_store_shm_file_path(file),
                        path->len, file->path_len);
}

static ngx_int_t
ngx_http_cache_index_store_shm_key_cmp(ngx_str_t *key,
                                       ngx_http_cache_index_shm_key_entry_t *entry) {
    return ngx_memn2cmp(key->data, entry->key, key->len, entry->key_len);
}

static ngx_int_t
ngx_http_cache_index_store_shm_tag_cmp(ngx_str_t *tag,
                                       ngx_http_cache_index_shm_tag_entry_t *entry) {
    return ngx_memn2cmp(tag->data, entry->tag, tag->len, entry->tag_len);
}

static ngx_int_t
ngx_http_cache_index_store_shm_link_exact_key_locked(
    ngx_http_cache_index_store_ctx_t *ctx,
    ngx_http_cache_index_shm_zone_t *zone,
    ngx_http_cache_index_shm_file_t *file) {
    ngx_http_cache_index_shm_key_entry_t *entry;
    ngx_str_t                             key;

    if (file->key_len == 0) {
        return NGX_OK;
    }

    key.data = ngx_http_cache_index_store_shm_file_key(file);
    key.len = file->key_len;
    entry = ngx_http_cache_index_store_shm_get_key_entry_locked(ctx, zone, &key,
            1);
    if (entry == NULL) {
        return NGX_ERROR;
    }

    file->key_entry = entry;
    ngx_queue_insert_tail(&entry->files, &file->key_queue);

    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_index_store_shm_link_tags_locked(
    ngx_http_cache_index_store_ctx_t *ctx,
    ngx_http_cache_index_shm_zone_t *zone,
    ngx_http_cache_index_shm_file_t *file, ngx_array_t *tags) {
    ngx_http_cache_index_shm_tag_entry_t  *entry;
    ngx_http_cache_index_shm_tag_member_t *member;
    ngx_str_t                             *tag;
    ngx_uint_t                             i;

    if (tags == NULL || tags->nelts == 0) {
        return NGX_OK;
    }

    tag = tags->elts;
    for (i = 0; i < tags->nelts; i++) {
        entry = ngx_http_cache_index_store_shm_get_tag_entry_locked(ctx, zone,
                &tag[i], 1);
        if (entry == NULL) {
            return NGX_ERROR;
        }

        member = ngx_http_cache_index_store_shm_alloc_locked(ctx,
                 sizeof(ngx_http_cache_index_shm_tag_member_t));
        if (member == NULL) {
            return NGX_ERROR;
        }

        ngx_memzero(member, sizeof(ngx_http_cache_index_shm_tag_member_t));
        member->tag_entry = entry;
        member->file = file;
        ngx_queue_insert_tail(&entry->files, &member->tag_queue);
        ngx_queue_insert_tail(&file->tag_members, &member->file_queue);
    }

    return NGX_OK;
}

static void
ngx_http_cache_index_store_shm_free_empty_key_entry_locked(
    ngx_http_cache_index_store_ctx_t *ctx,
    ngx_http_cache_index_shm_zone_t *zone,
    ngx_http_cache_index_shm_key_entry_t *entry) {
    if (!ngx_queue_empty(&entry->files)) {
        return;
    }

    ngx_rbtree_delete(&zone->key_index, &entry->node);
    ngx_slab_free_locked(ctx->shpool, entry);
}

static void
ngx_http_cache_index_store_shm_free_empty_tag_entry_locked(
    ngx_http_cache_index_store_ctx_t *ctx,
    ngx_http_cache_index_shm_zone_t *zone,
    ngx_http_cache_index_shm_tag_entry_t *entry) {
    if (!ngx_queue_empty(&entry->files)) {
        return;
    }

    ngx_rbtree_delete(&zone->tag_index, &entry->node);
    ngx_slab_free_locked(ctx->shpool, entry);
}

static ngx_int_t
ngx_http_cache_index_store_shm_upsert_file_meta(ngx_http_cache_index_store_t *store,
        ngx_str_t *zone_name, ngx_str_t *path, ngx_str_t *cache_key_text,
        time_t mtime, off_t size, ngx_array_t *tags, ngx_log_t *log) {
    ngx_http_cache_index_store_ctx_t  *ctx;
    ngx_http_cache_index_shm_zone_t   *zone;
    ngx_http_cache_index_shm_file_t   *file;
    ngx_str_t                         *tag;
    ngx_uint_t                         i;
    size_t                             alloc_size, tag_bytes, copied;
    u_char                            *p;
    ngx_flag_t                         locked;

    (void) log;

    ctx = store->ctx;
    locked = ngx_http_cache_index_store_shm_lock(store);

    zone = ngx_http_cache_index_store_shm_get_zone_locked(ctx, zone_name, 1);
    if (zone == NULL) {
        ngx_http_cache_index_store_shm_unlock(store, locked);
        return NGX_ERROR;
    }

    file = ngx_http_cache_index_store_shm_lookup_file(zone, path);
    if (file != NULL) {
        ngx_http_cache_index_store_shm_remove_file_locked(ctx, zone, file);
    }

    tag_bytes = 0;
    if (tags != NULL && tags->nelts > 0) {
        tag = tags->elts;
        for (i = 0; i < tags->nelts; i++) {
            tag_bytes += tag[i].len + 1;
        }
    }

    alloc_size = sizeof(ngx_http_cache_index_shm_file_t)
                 + path->len + 1
                 + cache_key_text->len + 1
                 + tag_bytes;

    file = ngx_http_cache_index_store_shm_alloc_locked(ctx, alloc_size);
    if (file == NULL) {
        ngx_http_cache_index_store_shm_unlock(store, locked);
        return NGX_ERROR;
    }

    ngx_memzero(file, sizeof(ngx_http_cache_index_shm_file_t));
    file->mtime = mtime;
    file->size = size;
    file->path_len = path->len;
    file->key_len = cache_key_text->len;
    file->tag_count = tags != NULL ? tags->nelts : 0;
    file->path_node.key = ngx_crc32_short(path->data, path->len);
    ngx_queue_init(&file->tag_members);

    p = file->data;
    p = ngx_cpymem(p, path->data, path->len);
    *p++ = '\0';
    p = ngx_cpymem(p, cache_key_text->data, cache_key_text->len);
    *p++ = '\0';

    if (tags != NULL && tags->nelts > 0) {
        tag = tags->elts;
        copied = 0;
        for (i = 0; i < tags->nelts; i++) {
            p = ngx_cpymem(p, tag[i].data, tag[i].len);
            *p++ = '\0';
            copied += tag[i].len + 1;
        }

        (void) copied;
    }

    ngx_rbtree_insert(&zone->path_index, &file->path_node);
    ngx_queue_insert_tail(&zone->files, &file->queue);

    if (ngx_http_cache_index_store_shm_link_exact_key_locked(ctx, zone, file)
            != NGX_OK) {
        ngx_http_cache_index_store_shm_remove_file_locked(ctx, zone, file);
        ngx_http_cache_index_store_shm_unlock(store, locked);
        return NGX_ERROR;
    }

    if (ngx_http_cache_index_store_shm_link_tags_locked(ctx, zone, file, tags)
            != NGX_OK) {
        ngx_http_cache_index_store_shm_remove_file_locked(ctx, zone, file);
        ngx_http_cache_index_store_shm_unlock(store, locked);
        return NGX_ERROR;
    }

    ngx_http_cache_index_store_shm_touch_zone_locked(zone);

    ngx_http_cache_index_store_shm_unlock(store, locked);

    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_index_store_shm_delete_file(ngx_http_cache_index_store_t *store,
        ngx_str_t *zone_name, ngx_str_t *path,
        ngx_log_t *log) {
    ngx_http_cache_index_store_ctx_t  *ctx;
    ngx_http_cache_index_shm_zone_t   *zone;
    ngx_http_cache_index_shm_file_t   *file;
    ngx_flag_t                         locked;

    (void) log;

    ctx = store->ctx;
    locked = ngx_http_cache_index_store_shm_lock(store);

    zone = ngx_http_cache_index_store_shm_lookup_zone(ctx->sh, zone_name);
    if (zone != NULL) {
        file = ngx_http_cache_index_store_shm_lookup_file(zone, path);
        if (file != NULL) {
            ngx_http_cache_index_store_shm_remove_file_locked(ctx, zone, file);
            ngx_http_cache_index_store_shm_touch_zone_locked(zone);
        }
    }

    ngx_http_cache_index_store_shm_unlock(store, locked);

    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_index_store_shm_push_path_unique(ngx_pool_t *pool,
        ngx_array_t *paths, u_char *data,
        size_t len) {
    ngx_str_t   *path;
    ngx_uint_t   i;

    path = paths->elts;
    for (i = 0; i < paths->nelts; i++) {
        if (path[i].len == len && ngx_strncmp(path[i].data, data, len) == 0) {
            return NGX_OK;
        }
    }

    path = ngx_array_push(paths);
    if (path == NULL) {
        return NGX_ERROR;
    }

    path->data = ngx_pnalloc(pool, len + 1);
    if (path->data == NULL) {
        return NGX_ERROR;
    }

    ngx_memcpy(path->data, data, len);
    path->len = len;
    path->data[len] = '\0';

    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_index_store_shm_collect_paths_by_tags(ngx_http_cache_index_store_t *store,
        ngx_pool_t *pool, ngx_str_t *zone_name, ngx_array_t *tags,
        ngx_array_t **paths, ngx_log_t *log) {
    ngx_http_cache_index_store_ctx_t  *ctx;
    ngx_http_cache_index_shm_zone_t   *zone;
    ngx_http_cache_index_shm_tag_entry_t *entry;
    ngx_http_cache_index_shm_tag_member_t *member;
    ngx_str_t                         *tag;
    ngx_queue_t                       *q;
    ngx_uint_t                         i;
    ngx_array_t                       *result;

    (void) log;

    result = ngx_array_create(pool, 4, sizeof(ngx_str_t));
    if (result == NULL) {
        return NGX_ERROR;
    }

    *paths = result;
    ctx = store->ctx;

    ngx_shmtx_lock(&ctx->shpool->mutex);
    zone = ngx_http_cache_index_store_shm_lookup_zone(ctx->sh, zone_name);
    if (zone != NULL && tags != NULL && tags->nelts > 0) {
        tag = tags->elts;
        for (i = 0; i < tags->nelts; i++) {
            entry = ngx_http_cache_index_store_shm_lookup_tag_entry(zone, &tag[i]);
            if (entry == NULL) {
                continue;
            }

            for (q = ngx_queue_head(&entry->files);
                    q != ngx_queue_sentinel(&entry->files);
                    q = ngx_queue_next(q)) {
                member = ngx_queue_data(q, ngx_http_cache_index_shm_tag_member_t,
                                        tag_queue);
                if (ngx_http_cache_index_store_shm_push_path_unique(pool, result,
                        ngx_http_cache_index_store_shm_file_path(member->file),
                        member->file->path_len) != NGX_OK) {
                    ngx_shmtx_unlock(&ctx->shpool->mutex);
                    return NGX_ERROR;
                }
            }
        }
    }
    ngx_shmtx_unlock(&ctx->shpool->mutex);

    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_index_store_shm_collect_paths_by_exact_key(
    ngx_http_cache_index_store_t *store, ngx_pool_t *pool,
    ngx_str_t *zone_name, ngx_str_t *key_text, ngx_array_t **paths,
    ngx_log_t *log) {
    ngx_http_cache_index_store_ctx_t  *ctx;
    ngx_http_cache_index_shm_zone_t   *zone;
    ngx_http_cache_index_shm_key_entry_t *entry;
    ngx_http_cache_index_shm_file_t   *file;
    ngx_queue_t                       *q;
    ngx_array_t                       *result;

    (void) log;

    result = ngx_array_create(pool, 4, sizeof(ngx_str_t));
    if (result == NULL) {
        return NGX_ERROR;
    }

    *paths = result;
    ctx = store->ctx;

    ngx_shmtx_lock(&ctx->shpool->mutex);
    zone = ngx_http_cache_index_store_shm_lookup_zone(ctx->sh, zone_name);
    if (zone != NULL) {
        entry = ngx_http_cache_index_store_shm_lookup_key_entry(zone, key_text);
        if (entry != NULL) {
            for (q = ngx_queue_head(&entry->files);
                    q != ngx_queue_sentinel(&entry->files);
                    q = ngx_queue_next(q)) {
                file = ngx_queue_data(q, ngx_http_cache_index_shm_file_t,
                                      key_queue);

                if (ngx_http_cache_index_store_shm_push_path_unique(pool, result,
                        ngx_http_cache_index_store_shm_file_path(file),
                        file->path_len) != NGX_OK) {
                    ngx_shmtx_unlock(&ctx->shpool->mutex);
                    return NGX_ERROR;
                }
            }
        }
    }
    ngx_shmtx_unlock(&ctx->shpool->mutex);

    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_index_store_shm_collect_paths_by_key_prefix(
    ngx_http_cache_index_store_t *store, ngx_pool_t *pool,
    ngx_str_t *zone_name, ngx_str_t *prefix, ngx_array_t **paths,
    ngx_log_t *log) {
    ngx_http_cache_index_store_ctx_t  *ctx;
    ngx_http_cache_index_shm_zone_t   *zone;
    ngx_http_cache_index_shm_file_t   *file;
    ngx_queue_t                       *q;
    ngx_array_t                       *result;

    (void) log;

    result = ngx_array_create(pool, 4, sizeof(ngx_str_t));
    if (result == NULL) {
        return NGX_ERROR;
    }

    *paths = result;
    ctx = store->ctx;

    ngx_shmtx_lock(&ctx->shpool->mutex);
    zone = ngx_http_cache_index_store_shm_lookup_zone(ctx->sh, zone_name);
    if (zone != NULL) {
        for (q = ngx_queue_head(&zone->files);
                q != ngx_queue_sentinel(&zone->files);
                q = ngx_queue_next(q)) {
            file = ngx_queue_data(q, ngx_http_cache_index_shm_file_t, queue);
            if (file->key_len < prefix->len
                    || ngx_strncmp(ngx_http_cache_index_store_shm_file_key(file),
                                   prefix->data, prefix->len) != 0) {
                continue;
            }

            if (ngx_http_cache_index_store_shm_push_path_unique(pool, result,
                    ngx_http_cache_index_store_shm_file_path(file),
                    file->path_len) != NGX_OK) {
                ngx_shmtx_unlock(&ctx->shpool->mutex);
                return NGX_ERROR;
            }
        }
    }
    ngx_shmtx_unlock(&ctx->shpool->mutex);

    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_index_store_shm_get_zone_state(ngx_http_cache_index_store_t *store,
        ngx_str_t *zone_name, ngx_http_cache_index_zone_state_t *state,
        ngx_log_t *log) {
    ngx_http_cache_index_store_ctx_t  *ctx;
    ngx_http_cache_index_shm_zone_t   *zone;

    (void) log;

    state->bootstrap_complete = 0;
    state->last_bootstrap_at = 0;
    state->last_updated_at = 0;

    ctx = store->ctx;
    ngx_shmtx_lock(&ctx->shpool->mutex);
    zone = ngx_http_cache_index_store_shm_lookup_zone(ctx->sh, zone_name);
    if (zone != NULL) {
        state->bootstrap_complete = zone->bootstrap_complete;
        state->last_bootstrap_at = zone->last_bootstrap_at;
        state->last_updated_at = zone->last_updated_at;
    }
    ngx_shmtx_unlock(&ctx->shpool->mutex);

    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_index_store_shm_set_zone_state(ngx_http_cache_index_store_t *store,
        ngx_str_t *zone_name, ngx_http_cache_index_zone_state_t *state,
        ngx_log_t *log) {
    ngx_http_cache_index_store_ctx_t  *ctx;
    ngx_http_cache_index_shm_zone_t   *zone;
    ngx_flag_t                         locked;

    (void) log;

    ctx = store->ctx;
    locked = ngx_http_cache_index_store_shm_lock(store);

    zone = ngx_http_cache_index_store_shm_get_zone_locked(ctx, zone_name, 1);
    if (zone == NULL) {
        ngx_http_cache_index_store_shm_unlock(store, locked);
        return NGX_ERROR;
    }

    zone->bootstrap_complete = state->bootstrap_complete;
    zone->last_bootstrap_at = state->last_bootstrap_at;
    zone->last_updated_at = state->last_updated_at;

    ngx_http_cache_index_store_shm_unlock(store, locked);

    return NGX_OK;
}

ngx_int_t
ngx_http_cache_index_extract_tokens(ngx_pool_t *pool, u_char *value, size_t len,
                                    ngx_array_t *tags, ngx_log_t *log) {
    size_t  i, start, end;

    i = 0;
    while (i < len) {
        while (i < len && (value[i] == ' ' || value[i] == '\t'
                           || value[i] == '\r' || value[i] == '\n'
                           || value[i] == ',')) {
            i++;
        }

        start = i;

        while (i < len && value[i] != ' ' && value[i] != '\t'
                && value[i] != '\r' && value[i] != '\n'
                && value[i] != ',') {
            i++;
        }

        end = i;
        if (end > start) {
            if (tags->nelts >= NGX_HTTP_CACHE_TAG_MAX_TOKENS_PER_SCAN) {
                ngx_log_error(NGX_LOG_WARN, log, 0,
                              "cache tag: too many tags in response header, "
                              "truncating at %d",
                              NGX_HTTP_CACHE_TAG_MAX_TOKENS_PER_SCAN);
                break;
            }

            if (ngx_http_cache_index_push_unique(pool, tags, value + start,
                                                 end - start) != NGX_OK) {
                return NGX_ERROR;
            }
        }
    }

    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_index_push_unique(ngx_pool_t *pool, ngx_array_t *tags,
                                 u_char *data, size_t len) {
    ngx_str_t   *tag;
    ngx_uint_t   i;

    if (len == 0) {
        return NGX_OK;
    }

    tag = tags->elts;
    for (i = 0; i < tags->nelts; i++) {
        if (tag[i].len == len
                && ngx_strncasecmp(tag[i].data, data, len) == 0) {
            return NGX_OK;
        }
    }

    tag = ngx_array_push(tags);
    if (tag == NULL) {
        return NGX_ERROR;
    }

    tag->data = ngx_pnalloc(pool, len);
    if (tag->data == NULL) {
        return NGX_ERROR;
    }

    ngx_memcpy(tag->data, data, len);
    tag->len = len;

    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_index_path_known(ngx_str_t *path) {
    u_char  *p;

    p = path->data + path->len;
    while (p > path->data && p[-1] != '/') {
        p--;
    }

    return (size_t)(path->data + path->len - p) == 2 * NGX_HTTP_CACHE_KEY_LEN;
}

static ngx_int_t
ngx_http_cache_index_parse_file(ngx_pool_t *pool, ngx_str_t *path,
                                ngx_array_t *headers, ngx_array_t **tags,
                                ngx_str_t *cache_key_text,
                                time_t *mtime, off_t *size, ngx_log_t *log) {
    static u_char        key_hdr[] = "KEY:";
    ngx_file_info_t      fi;
    ngx_file_t           file;
    u_char              *buf;
    ssize_t              n;
    size_t               max_read, i, j, line_end, value_start;
    ngx_array_t         *result;
    ngx_str_t           *header;

    if (!ngx_http_cache_index_path_known(path)) {
        return NGX_DECLINED;
    }

    if (ngx_file_info(path->data, &fi) == NGX_FILE_ERROR) {
        return NGX_DECLINED;
    }

    max_read = (size_t) ngx_min((off_t) 65536, ngx_file_size(&fi));
    if (max_read == 0) {
        return NGX_DECLINED;
    }

    buf = ngx_pnalloc(pool, max_read);
    if (buf == NULL) {
        return NGX_ERROR;
    }

    ngx_memzero(&file, sizeof(ngx_file_t));
    file.name = *path;
    file.log = log;
    file.fd = ngx_open_file(path->data, NGX_FILE_RDONLY, NGX_FILE_OPEN, 0);
    if (file.fd == NGX_INVALID_FILE) {
        return NGX_DECLINED;
    }

    n = ngx_read_file(&file, buf, max_read, 0);
    ngx_close_file(file.fd);

    if (n == NGX_ERROR || n <= 0) {
        return NGX_DECLINED;
    }

    result = ngx_array_create(pool, 4, sizeof(ngx_str_t));
    if (result == NULL) {
        return NGX_ERROR;
    }

    for (i = 0; i < (size_t) n; i++) {
        if (i != 0 && buf[i - 1] != '\n') {
            continue;
        }

        /* Extract KEY: line (capture whole value, not token-split) */
        if (cache_key_text->len == 0
                && i + 4 <= (size_t) n
                && ngx_strncasecmp(buf + i, key_hdr, 4) == 0) {
            value_start = i + 4;
            while (value_start < (size_t) n
                    && (buf[value_start] == ' ' || buf[value_start] == '\t')) {
                value_start++;
            }
            line_end = value_start;
            while (line_end < (size_t) n
                    && buf[line_end] != '\n' && buf[line_end] != '\r') {
                line_end++;
            }
            if (line_end > value_start) {
                cache_key_text->data = ngx_pnalloc(pool,
                                                   line_end - value_start + 1);
                if (cache_key_text->data == NULL) {
                    return NGX_ERROR;
                }
                ngx_memcpy(cache_key_text->data, buf + value_start,
                           line_end - value_start);
                cache_key_text->len = line_end - value_start;
                cache_key_text->data[cache_key_text->len] = '\0';
            }
        }

        if (headers == NULL) {
            continue;
        }

        header = headers->elts;
        for (j = 0; j < headers->nelts; j++) {
            if (i + header[j].len + 1 >= (size_t) n) {
                continue;
            }

            if (ngx_strncasecmp(buf + i, header[j].data, header[j].len) != 0
                    || buf[i + header[j].len] != ':') {
                continue;
            }

            value_start = i + header[j].len + 1;
            while (value_start < (size_t) n
                    && (buf[value_start] == ' ' || buf[value_start] == '\t')) {
                value_start++;
            }

            line_end = value_start;
            while (line_end < (size_t) n && buf[line_end] != '\n'
                    && buf[line_end] != '\r') {
                line_end++;
            }

            if (ngx_http_cache_index_extract_tokens(pool, buf + value_start,
                                                    line_end - value_start,
                                                    result, log) != NGX_OK) {
                return NGX_ERROR;
            }
        }
    }

    *mtime = ngx_file_mtime(&fi);
    *size = ngx_file_size(&fi);
    *tags = result;

    return NGX_OK;
}

#endif
