#include "ngx_cache_pilot_index.h"

#if (NGX_LINUX)

static ngx_http_cache_index_watch_runtime_t ngx_http_cache_index_watch_runtime;

static void ngx_http_cache_index_zone_insert(ngx_rbtree_node_t *temp,
        ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel);
static ngx_http_cache_index_zone_index_t *ngx_http_cache_index_lookup_zone_index(
    ngx_http_file_cache_t *cache);
static void ngx_http_cache_index_zone_state_cache_set(ngx_http_file_cache_t *cache,
        ngx_http_cache_index_zone_state_t *state);
static ngx_int_t ngx_http_cache_index_zone_state_cache_sync(ngx_cycle_t *cycle,
        ngx_http_cache_pilot_main_conf_t *pmcf);
static ngx_int_t ngx_http_cache_index_scan_zone(
    ngx_http_cache_index_store_t *store, ngx_http_cache_index_zone_t *zone,
    ngx_cycle_t *cycle);
static ngx_int_t ngx_http_cache_index_scan_recursive(
    ngx_http_cache_index_store_t *store, ngx_http_cache_index_zone_t *zone,
    ngx_pool_t *pool, ngx_str_t *path, ngx_cycle_t *cycle, time_t min_mtime);
static ngx_int_t ngx_http_cache_index_join_path(ngx_pool_t *pool, ngx_str_t *base,
        const char *name, ngx_str_t *out);
static ngx_int_t ngx_http_cache_index_runtime_init_zones(
    ngx_cycle_t *cycle, ngx_http_cache_pilot_main_conf_t *pmcf);


ngx_flag_t
ngx_http_cache_index_is_owner(void) {
    return ngx_http_cache_index_watch_runtime.owner;
}

ngx_http_cache_index_zone_t *
ngx_http_cache_index_lookup_zone(ngx_http_file_cache_t *cache) {
    ngx_http_cache_index_zone_index_t  *index;

    index = ngx_http_cache_index_lookup_zone_index(cache);
    if (index == NULL) {
        return NULL;
    }

    return index->zone;
}

ngx_flag_t
ngx_http_cache_index_zone_bootstrap_complete(ngx_http_file_cache_t *cache) {
    ngx_http_cache_index_zone_index_t *index;

    index = ngx_http_cache_index_lookup_zone_index(cache);
    if (index == NULL) {
        return 0;
    }

    return index->bootstrap_complete;
}

ngx_flag_t
ngx_http_cache_index_zone_bootstrap_complete_sync(
    ngx_http_cache_pilot_main_conf_t *pmcf,
    ngx_http_file_cache_t *cache,
    ngx_log_t *log) {
    ngx_http_cache_index_zone_t        *zone;
    ngx_http_cache_index_store_t       *reader;
    ngx_http_cache_index_zone_state_t   state;

    if (ngx_http_cache_index_zone_bootstrap_complete(cache)) {
        return 1;
    }

    if (pmcf == NULL || cache == NULL) {
        return 0;
    }

    zone = ngx_http_cache_index_lookup_zone(cache);
    if (zone == NULL) {
        return 0;
    }

    reader = ngx_http_cache_index_store_reader(pmcf, log);
    if (reader == NULL) {
        return 0;
    }

    state.bootstrap_complete = 0;
    state.last_bootstrap_at = 0;
    state.last_updated_at = 0;

    if (ngx_http_cache_index_store_get_zone_state(reader, &zone->zone_name,
            &state, log) != NGX_OK) {
        return 0;
    }

    ngx_http_cache_index_zone_state_cache_set(cache, &state);

    return state.bootstrap_complete ? 1 : 0;
}

ngx_int_t
ngx_http_cache_index_bootstrap_zone(ngx_http_cache_index_store_t *store,
                                    ngx_http_cache_index_zone_t *zone,
                                    ngx_cycle_t *cycle) {
    ngx_http_cache_index_zone_state_t  state;

    if (ngx_http_cache_index_scan_zone(store, zone, cycle) != NGX_OK) {
        return NGX_ERROR;
    }

    state.bootstrap_complete = 1;
    state.last_bootstrap_at = ngx_time();
    state.last_updated_at = state.last_bootstrap_at;
    if (ngx_http_cache_index_store_set_zone_state(store, &zone->zone_name, &state,
            cycle->log) != NGX_OK) {
        return NGX_ERROR;
    }

    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_index_scan_zone(ngx_http_cache_index_store_t *store,
                               ngx_http_cache_index_zone_t *zone,
                               ngx_cycle_t *cycle) {
    ngx_pool_t *pool;

    if (zone == NULL || zone->cache == NULL || zone->cache->path == NULL) {
        return NGX_DECLINED;
    }

    ngx_log_error(NGX_LOG_NOTICE, cycle->log, 0,
                  "cache_tag bootstrap zone \"%V\"", &zone->zone_name);

    pool = ngx_create_pool(4096, cycle->log);
    if (pool == NULL) {
        return NGX_ERROR;
    }

    if (ngx_http_cache_index_scan_recursive(store, zone, pool,
                                            &zone->cache->path->name, cycle, 0) != NGX_OK) {
        ngx_destroy_pool(pool);
        return NGX_ERROR;
    }

    ngx_destroy_pool(pool);

    return NGX_OK;
}

static void
ngx_http_cache_index_zone_insert(ngx_rbtree_node_t *temp, ngx_rbtree_node_t *node,
                                 ngx_rbtree_node_t *sentinel) {
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

        if (((ngx_http_cache_index_zone_index_t *) node)->zone->cache
                < ((ngx_http_cache_index_zone_index_t *) temp)->zone->cache) {
            if (temp->left == sentinel) {
                temp->left = node;
                break;
            }
            temp = temp->left;
        } else {
            if (temp->right == sentinel) {
                temp->right = node;
                break;
            }
            temp = temp->right;
        }
    }

    node->parent = temp;
    node->left = sentinel;
    node->right = sentinel;
    ngx_rbt_red(node);
}

static ngx_http_cache_index_zone_index_t *
ngx_http_cache_index_lookup_zone_index(ngx_http_file_cache_t *cache) {
    ngx_rbtree_node_t              *node, *sentinel;
    ngx_rbtree_key_t                key;
    ngx_http_cache_index_zone_index_t *index;

    node = ngx_http_cache_index_watch_runtime.zone_index.root;
    sentinel = ngx_http_cache_index_watch_runtime.zone_index.sentinel;
    key = (ngx_rbtree_key_t)(uintptr_t) cache;

    while (node != sentinel) {
        if (key < node->key) {
            node = node->left;
            continue;
        }

        if (key > node->key) {
            node = node->right;
            continue;
        }

        index = (ngx_http_cache_index_zone_index_t *) node;
        if (index->zone->cache == cache) {
            return index;
        }

        node = cache < index->zone->cache ? node->left : node->right;
    }

    return NULL;
}

static ngx_int_t
ngx_http_cache_index_scan_recursive(ngx_http_cache_index_store_t *store,
                                    ngx_http_cache_index_zone_t *zone,
                                    ngx_pool_t *pool, ngx_str_t *path, ngx_cycle_t *cycle,
                                    time_t min_mtime) {
    DIR            *dir;
    struct dirent  *entry;
    ngx_str_t       child;
    ngx_file_info_t fi;

    dir = opendir((const char *) path->data);
    if (dir == NULL) {
        return NGX_DECLINED;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (ngx_strcmp(entry->d_name, ".") == 0
                || ngx_strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        if (ngx_http_cache_index_join_path(pool, path, entry->d_name, &child)
                != NGX_OK) {
            closedir(dir);
            return NGX_ERROR;
        }

        if (entry->d_type == DT_DIR) {
            if (ngx_http_cache_index_scan_recursive(store, zone, pool, &child, cycle,
                                                    min_mtime) == NGX_ERROR) {
                closedir(dir);
                return NGX_ERROR;
            }
            continue;
        }

        if (entry->d_type != DT_REG && entry->d_type != DT_UNKNOWN) {
            continue;
        }

        if (min_mtime > 0) {
            if (ngx_file_info(child.data, &fi) == NGX_FILE_ERROR) {
                continue;
            }

            if (ngx_file_mtime(&fi) <= min_mtime) {
                continue;
            }
        }

        if (ngx_http_cache_index_store_process_file(store, &zone->zone_name,
                &child, zone->headers, cycle->log) == NGX_ERROR) {
            closedir(dir);
            return NGX_ERROR;
        }
    }

    closedir(dir);

    return NGX_OK;
}

#if (NGX_CACHE_PILOT_THREADS)

typedef struct {
    ngx_cycle_t                       *cycle;
    ngx_http_cache_pilot_main_conf_t  *pmcf;
    ngx_int_t                          rc;
} ngx_http_cache_index_bootstrap_ctx_t;

static void
ngx_http_cache_index_bootstrap_thread(void *data, ngx_log_t *log) {
    ngx_http_cache_index_bootstrap_ctx_t  *ctx;
    ngx_http_cache_index_store_t          *writer;
    ngx_http_cache_index_zone_t           *zone;
    ngx_http_cache_index_zone_state_t      state;
    ngx_uint_t                             i;

    ctx = data;

    writer = ngx_http_cache_index_store_open_writer(ctx->pmcf, log);
    if (writer == NULL) {
        ctx->rc = NGX_ERROR;
        return;
    }

    zone = ctx->pmcf->zones->elts;
    for (i = 0; i < ctx->pmcf->zones->nelts; i++) {
        if (zone[i].cache == NULL || zone[i].cache->path == NULL) {
            continue;
        }

        if (ngx_http_cache_index_store_get_zone_state(writer, &zone[i].zone_name,
                &state, log) != NGX_OK) {
            ngx_log_error(NGX_LOG_WARN, log, 0,
                          "cache_tag: cannot read zone state for \"%V\" "
                          "in bootstrap thread, skipping",
                          &zone[i].zone_name);
            ctx->rc = NGX_ERROR;
            continue;
        }

        if (state.bootstrap_complete) {
            ngx_pool_t *pool;

            ngx_log_error(NGX_LOG_NOTICE, log, 0,
                          "cache_tag reusing persisted index for zone \"%V\"",
                          &zone[i].zone_name);
            pool = ngx_create_pool(4096, log);
            if (pool == NULL) {
                ctx->rc = NGX_ERROR;
                continue;
            }

            if (ngx_http_cache_index_scan_recursive(writer, &zone[i], pool,
                                                    &zone[i].cache->path->name, ctx->cycle,
                                                    state.last_bootstrap_at) == NGX_ERROR) {
                ngx_log_error(NGX_LOG_WARN, log, 0,
                              "cache_tag: bootstrap scan failed for zone \"%V\" "
                              "in bootstrap thread, continuing",
                              &zone[i].zone_name);
                ctx->rc = NGX_ERROR;
            }

            ngx_destroy_pool(pool);

            state.last_bootstrap_at = ngx_time();
            if (ngx_http_cache_index_store_set_zone_state(writer,
                    &zone[i].zone_name, &state, log) != NGX_OK) {
                ngx_log_error(NGX_LOG_WARN, log, 0,
                              "cache_tag: cannot update zone state for \"%V\" "
                              "in bootstrap thread",
                              &zone[i].zone_name);
                ctx->rc = NGX_ERROR;
            }
            continue;
        }

        if (ngx_http_cache_index_bootstrap_zone(writer, &zone[i],
                                                ctx->cycle) != NGX_OK) {
            ngx_log_error(NGX_LOG_WARN, log, 0,
                          "cache_tag: bootstrap failed for zone \"%V\" "
                          "in bootstrap thread, deferring until first request",
                          &zone[i].zone_name);
            ctx->rc = NGX_ERROR;
        }
    }
}

static void
ngx_http_cache_index_bootstrap_complete(ngx_event_t *ev) {
    ngx_http_cache_index_bootstrap_ctx_t  *ctx;
    ngx_cycle_t                           *cycle;

    ctx = ev->data;
    cycle = ctx->cycle;

    if (ctx->rc != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
                      "cache_tag: background bootstrap failed, "
                      "continuing with partial index");
    }

    if (ngx_http_cache_index_zone_state_cache_sync(cycle, ctx->pmcf) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, cycle->log, 0,
                      "cache_tag: failed to sync zone state cache after bootstrap");
    }

    ngx_http_cache_index_watch_runtime.active = 1;

    ngx_log_error(NGX_LOG_NOTICE, cycle->log, 0,
                  "cache_tag bootstrap complete");
}

#endif /* NGX_CACHE_PILOT_THREADS */


ngx_int_t
ngx_http_cache_index_init_runtime(ngx_cycle_t *cycle,
                                  ngx_http_cache_pilot_main_conf_t *pmcf) {
    ngx_http_cache_index_zone_t        *zone;
    ngx_http_cache_index_store_t       *writer;
    ngx_http_cache_index_zone_state_t   state;
    ngx_uint_t                          i;
#if (NGX_CACHE_PILOT_THREADS)
    ngx_thread_pool_t                  *tp;
    ngx_thread_task_t                  *task;
    ngx_http_cache_index_bootstrap_ctx_t *bctx;
    static ngx_str_t                    default_pool_name = ngx_string("default");
#endif

    ngx_memzero(&ngx_http_cache_index_watch_runtime,
                sizeof(ngx_http_cache_index_watch_runtime));

    ngx_rbtree_init(&ngx_http_cache_index_watch_runtime.zone_index,
                    &ngx_http_cache_index_watch_runtime.zone_sentinel,
                    (ngx_rbtree_insert_pt) ngx_http_cache_index_zone_insert);

    ngx_http_cache_index_watch_runtime.owner = (
                ngx_process == NGX_PROCESS_SINGLE
                || (ngx_process == NGX_PROCESS_WORKER && ngx_worker == 0));
    ngx_http_cache_index_watch_runtime.cycle = cycle;

    if (ngx_http_cache_index_runtime_init_zones(cycle, pmcf) != NGX_OK) {
        return NGX_ERROR;
    }

    /*
     * Initialise the store for all worker processes so that the header-filter
     * log handler can write index entries from whichever worker handles the
     * upstream response.  Pass owner=1 so every worker gets a writer handle;
     * the SHM slab mutex serialises concurrent updates.
     */
    if (ngx_http_cache_index_store_runtime_init(cycle, pmcf, 1) != NGX_OK) {
        return NGX_ERROR;
    }

    if (ngx_http_cache_index_zone_state_cache_sync(cycle, pmcf) != NGX_OK) {
        return NGX_ERROR;
    }

    /* Non-owner workers have their store ready; the bootstrap scan runs only
     * in the owner worker to avoid redundant directory walks. */
    if (!ngx_http_cache_index_watch_runtime.owner || pmcf->zones->nelts == 0) {
        ngx_http_cache_index_watch_runtime.initialized = 1;
        return NGX_OK;
    }

#if (NGX_CACHE_PILOT_THREADS)
    tp = ngx_thread_pool_get(cycle, &default_pool_name);
    if (tp != NULL) {
        task = ngx_thread_task_alloc(cycle->pool,
                                     sizeof(ngx_http_cache_index_bootstrap_ctx_t));
        if (task != NULL) {
            bctx = task->ctx;
            bctx->cycle = cycle;
            bctx->pmcf  = pmcf;
            bctx->rc    = NGX_OK;
            task->handler       = ngx_http_cache_index_bootstrap_thread;
            task->event.handler = ngx_http_cache_index_bootstrap_complete;
            task->event.data    = bctx;
            if (ngx_thread_task_post(tp, task) == NGX_OK) {
                ngx_http_cache_index_watch_runtime.initialized = 1;
                return NGX_OK;
            }
        }
    }
#endif

    writer = ngx_http_cache_index_store_writer();
    zone = pmcf->zones->elts;
    for (i = 0; i < pmcf->zones->nelts; i++) {
        if (zone[i].cache == NULL || zone[i].cache->path == NULL) {
            continue;
        }

        if (ngx_http_cache_index_store_get_zone_state(writer, &zone[i].zone_name,
                &state, cycle->log) != NGX_OK) {
            ngx_log_error(NGX_LOG_WARN, cycle->log, 0,
                          "cache_tag: cannot read zone state for \"%V\" "
                          "at startup, deferring bootstrap to first request",
                          &zone[i].zone_name);
            continue;
        }

        if (state.bootstrap_complete) {
            ngx_pool_t *pool;

            ngx_log_error(NGX_LOG_NOTICE, cycle->log, 0,
                          "cache_tag reusing persisted index for zone \"%V\"",
                          &zone[i].zone_name);
            pool = ngx_create_pool(4096, cycle->log);
            if (pool == NULL) {
                return NGX_ERROR;
            }

            if (ngx_http_cache_index_scan_recursive(writer, &zone[i], pool,
                                                    &zone[i].cache->path->name, cycle,
                                                    state.last_bootstrap_at) == NGX_ERROR) {
                ngx_log_error(NGX_LOG_WARN, cycle->log, 0,
                              "cache_tag: bootstrap scan failed for zone \"%V\", "
                              "continuing without directory pre-scan",
                              &zone[i].zone_name);
            }

            ngx_destroy_pool(pool);

            state.last_bootstrap_at = ngx_time();
            if (ngx_http_cache_index_store_set_zone_state(writer, &zone[i].zone_name,
                    &state, cycle->log) != NGX_OK) {
                ngx_log_error(NGX_LOG_WARN, cycle->log, 0,
                              "cache_tag: cannot update zone state for \"%V\"",
                              &zone[i].zone_name);
            }

            ngx_http_cache_index_zone_state_cache_set(zone[i].cache, &state);
            continue;
        }

        if (ngx_http_cache_index_bootstrap_zone(writer, &zone[i], cycle) != NGX_OK) {
            ngx_log_error(NGX_LOG_WARN, cycle->log, 0,
                          "cache_tag: bootstrap failed for zone \"%V\", "
                          "deferring until first purge request",
                          &zone[i].zone_name);
        }
    }

    ngx_http_cache_index_watch_runtime.initialized = 1;
    ngx_http_cache_index_watch_runtime.active = 1;

    return NGX_OK;
}

ngx_int_t
ngx_http_cache_index_flush_pending(ngx_cycle_t *cycle) {
    (void) cycle;
    return NGX_OK;
}

void
ngx_http_cache_index_shutdown_runtime(void) {
    ngx_http_cache_index_store_runtime_shutdown();
    ngx_memzero(&ngx_http_cache_index_watch_runtime,
                sizeof(ngx_http_cache_index_watch_runtime));
}

static ngx_int_t
ngx_http_cache_index_join_path(ngx_pool_t *pool, ngx_str_t *base, const char *name,
                               ngx_str_t *out) {
    size_t  name_len;

    name_len = ngx_strlen(name);
    out->len = base->len + 1 + name_len;
    out->data = ngx_pnalloc(pool, out->len + 1);
    if (out->data == NULL) {
        return NGX_ERROR;
    }

    ngx_snprintf(out->data, out->len + 1, "%V/%s%Z", base, name);

    return NGX_OK;
}

static ngx_int_t
ngx_http_cache_index_runtime_init_zones(ngx_cycle_t *cycle,
                                        ngx_http_cache_pilot_main_conf_t *pmcf) {
    ngx_http_cache_index_zone_t        *zone;
    ngx_http_cache_index_zone_index_t  *index;
    ngx_uint_t                          i;

    zone = pmcf->zones->elts;
    for (i = 0; i < pmcf->zones->nelts; i++) {
        index = ngx_pcalloc(cycle->pool, sizeof(ngx_http_cache_index_zone_index_t));
        if (index == NULL) {
            return NGX_ERROR;
        }

        index->zone = &zone[i];
        index->bootstrap_complete = 0;
        index->last_bootstrap_at = 0;
        index->last_updated_at = 0;
        index->node.key = (ngx_rbtree_key_t)(uintptr_t) zone[i].cache;
        ngx_rbtree_insert(&ngx_http_cache_index_watch_runtime.zone_index,
                          &index->node);
    }

    return NGX_OK;
}

static void
ngx_http_cache_index_zone_state_cache_set(ngx_http_file_cache_t *cache,
        ngx_http_cache_index_zone_state_t *state) {
    ngx_http_cache_index_zone_index_t *index;

    if (cache == NULL || state == NULL) {
        return;
    }

    index = ngx_http_cache_index_lookup_zone_index(cache);
    if (index == NULL) {
        return;
    }

    index->bootstrap_complete = state->bootstrap_complete;
    index->last_bootstrap_at = state->last_bootstrap_at;
    index->last_updated_at = state->last_updated_at;
}

static ngx_int_t
ngx_http_cache_index_zone_state_cache_sync(ngx_cycle_t *cycle,
        ngx_http_cache_pilot_main_conf_t *pmcf) {
    ngx_http_cache_index_store_t       *reader;
    ngx_http_cache_index_zone_t        *zone;
    ngx_http_cache_index_zone_state_t   state;
    ngx_uint_t                          i;

    if (pmcf == NULL || pmcf->zones == NULL || pmcf->zones->nelts == 0) {
        return NGX_OK;
    }

    zone = pmcf->zones->elts;

    reader = ngx_http_cache_index_store_reader(pmcf, cycle->log);
    if (reader == NULL) {
        ngx_log_error(NGX_LOG_NOTICE, cycle->log, 0,
                      "cache_tag: index reader not ready at startup, "
                      "assuming zones are not bootstrapped yet");

        state.bootstrap_complete = 0;
        state.last_bootstrap_at = 0;
        state.last_updated_at = 0;

        for (i = 0; i < pmcf->zones->nelts; i++) {
            if (zone[i].cache == NULL) {
                continue;
            }
            ngx_http_cache_index_zone_state_cache_set(zone[i].cache, &state);
        }

        return NGX_OK;
    }

    for (i = 0; i < pmcf->zones->nelts; i++) {
        if (zone[i].cache == NULL) {
            continue;
        }

        state.bootstrap_complete = 0;
        state.last_bootstrap_at = 0;
        state.last_updated_at = 0;
        if (ngx_http_cache_index_store_get_zone_state(reader, &zone[i].zone_name,
                &state, cycle->log) != NGX_OK) {
            ngx_log_error(NGX_LOG_WARN, cycle->log, 0,
                          "cache_tag: cannot read initial zone state for \"%V\", "
                          "assuming not bootstrapped", &zone[i].zone_name);
            state.bootstrap_complete = 0;
            state.last_bootstrap_at = 0;
            state.last_updated_at = 0;
        }

        ngx_http_cache_index_zone_state_cache_set(zone[i].cache, &state);
    }

    return NGX_OK;
}

#endif
