/*
 * Cross-platform cache index updater.
 *
 * Replaces the Linux-only inotify watcher with two nginx-native hooks:
 *
 *  - Header filter: fires as nginx sends the upstream response headers.
 *    When the response is being written to cache (r->upstream->cacheable),
 *    the configured tag headers are extracted from r->headers_out and stashed
 *    on a per-request context.
 *
 *  - Log phase handler: fires after the response is fully sent.  If the
 *    context was stashed (indicating a cache write), the SHM index is updated
 *    directly - no file re-reading, no inotify FD, no delay.
 *
 * Cache manager expirations leave stale index entries; these are cleaned up
 * lazily when a tag purge attempts to delete an already-gone file (ENOENT is
 * already handled gracefully by the purge path).
 *
 * The cold-start bootstrap scan (reading existing cache files from disk) is
 * still performed on process init in ngx_cache_pilot_index_watch_linux.c.
 */

#include "ngx_cache_pilot_index.h"


typedef struct {
    ngx_array_t  *tags;
    ngx_str_t     zone_name;
} ngx_http_cache_index_req_ctx_t;


static ngx_http_output_header_filter_pt  ngx_http_next_cache_pilot_index_header_filter;

static ngx_int_t ngx_http_cache_pilot_index_response_headers(
    ngx_http_request_t *r, ngx_http_cache_pilot_loc_conf_t *cplcf,
    ngx_array_t **tags);
static ngx_int_t ngx_http_cache_pilot_index_header_filter(ngx_http_request_t *r);
static ngx_int_t ngx_http_cache_pilot_index_log_handler(ngx_http_request_t *r);


/*
 * Extract tag header values from r->headers_out.
 * Mirrors ngx_http_cache_index_request_headers() (index.c:44) but reads
 * the upstream response headers rather than the incoming request headers.
 */
static ngx_int_t
ngx_http_cache_pilot_index_response_headers(ngx_http_request_t *r,
        ngx_http_cache_pilot_loc_conf_t *cplcf, ngx_array_t **tags) {
    ngx_list_part_t  *part;
    ngx_table_elt_t  *header;
    ngx_str_t        *wanted;
    ngx_uint_t        i, j;
    ngx_array_t      *result;

    result = ngx_array_create(r->pool, 4, sizeof(ngx_str_t));
    if (result == NULL) {
        return NGX_ERROR;
    }

    part = &r->headers_out.headers.part;
    header = part->elts;
    wanted = cplcf->cache_tag_headers->elts;

    for (i = 0; ; i++) {
        if (i >= part->nelts) {
            if (part->next == NULL) {
                break;
            }
            part = part->next;
            header = part->elts;
            i = 0;
        }

        for (j = 0; j < cplcf->cache_tag_headers->nelts; j++) {
            if (header[i].key.len != wanted[j].len) {
                continue;
            }

            if (ngx_strncasecmp(header[i].key.data, wanted[j].data,
                                wanted[j].len) != 0) {
                continue;
            }

            if (ngx_http_cache_index_extract_tokens(r->pool,
                                                    header[i].value.data, header[i].value.len,
                                                    result, r->connection->log) != NGX_OK) {
                return NGX_ERROR;
            }
        }
    }

    *tags = result;
    return NGX_OK;
}


/*
 * Header filter: stash tags for any request that will write a cache entry.
 *
 * r->upstream->cacheable is set by the upstream module once it has decided
 * the response may be stored, so at this point the response headers are
 * available in r->headers_out and the decision to cache has been made.
 */
static ngx_int_t
ngx_http_cache_pilot_index_header_filter(ngx_http_request_t *r) {
#if (NGX_LINUX)
    ngx_http_cache_pilot_loc_conf_t  *cplcf;
    ngx_http_cache_index_req_ctx_t   *ctx;
    ngx_http_cache_index_zone_t      *zone;
    ngx_array_t                      *tags;

    if (r->cache == NULL
            || r->upstream == NULL
            || !r->upstream->cacheable) {
        goto done;
    }

    cplcf = ngx_http_get_module_loc_conf(r, ngx_http_cache_pilot_module);
    if (!ngx_http_cache_index_location_enabled(cplcf)) {
        goto done;
    }

    zone = ngx_http_cache_index_lookup_zone(r->cache->file_cache);
    if (zone == NULL) {
        goto done;
    }

    if (ngx_http_cache_index_store_writer() == NULL) {
        goto done;
    }

    if (ngx_http_cache_pilot_index_response_headers(r, cplcf, &tags) != NGX_OK) {
        return NGX_ERROR;
    }

    if (tags->nelts == 0) {
        goto done;
    }

    ctx = ngx_pcalloc(r->pool, sizeof(*ctx));
    if (ctx == NULL) {
        return NGX_ERROR;
    }

    ctx->tags = tags;
    ctx->zone_name = zone->zone_name;

    ngx_http_set_ctx(r, ctx, ngx_http_cache_pilot_module);

done:
#endif

    return ngx_http_next_cache_pilot_index_header_filter(r);
}


/*
 * Log phase handler: commit the SHM index entry for a freshly written cache
 * file.  Runs after the response is sent; the SHM write is in-memory and fast.
 *
 * We only proceed if the request context was stashed in the header filter,
 * which happens only for cacheable upstream responses - so we naturally skip
 * cache hits, purge requests, and any request that did not write a cache file.
 *
 * If the upstream response turned out to be non-cacheable despite the initial
 * header verdict (e.g. a downstream filter cleared cacheable), we silently
 * insert a stale entry; the purge code handles this gracefully via ENOENT.
 */
static ngx_int_t
ngx_http_cache_pilot_index_log_handler(ngx_http_request_t *r) {
#if (NGX_LINUX)
    ngx_http_cache_index_req_ctx_t   *ctx;
    ngx_http_cache_index_store_t     *writer;
    ngx_str_t                         cache_key;
    ngx_str_t                        *key_parts;
    size_t                            key_len;
    ngx_uint_t                        i;

    ctx = ngx_http_get_module_ctx(r, ngx_http_cache_pilot_module);
    if (ctx == NULL || ctx->tags == NULL || ctx->tags->nelts == 0) {
        return NGX_OK;
    }

    if (r->cache == NULL || r->cache->file.name.len == 0) {
        return NGX_OK;
    }

    writer = ngx_http_cache_index_store_writer();
    if (writer == NULL) {
        return NGX_OK;
    }

    /* Assemble the full cache key by concatenating all key components.
     * This matches the "KEY: ..." line written into the cache file header. */
    key_len = 0;
    key_parts = r->cache->keys.elts;
    for (i = 0; i < r->cache->keys.nelts; i++) {
        key_len += key_parts[i].len;
    }

    cache_key.data = ngx_pnalloc(r->pool, key_len + 1);
    if (cache_key.data == NULL) {
        return NGX_OK;
    }

    cache_key.len = 0;
    for (i = 0; i < r->cache->keys.nelts; i++) {
        ngx_memcpy(cache_key.data + cache_key.len,
                   key_parts[i].data, key_parts[i].len);
        cache_key.len += key_parts[i].len;
    }
    cache_key.data[cache_key.len] = '\0';

    ngx_log_debug2(NGX_LOG_DEBUG_HTTP, r->connection->log, 0,
                   "cache_tag index update zone:\"%V\" path:\"%V\"",
                   &ctx->zone_name, &r->cache->file.name);

    if (ngx_http_cache_index_store_upsert_file_meta(writer, &ctx->zone_name,
            &r->cache->file.name, &cache_key,
            ngx_time(), 0, ctx->tags, r->connection->log) != NGX_OK) {
        ngx_log_error(NGX_LOG_ERR, r->connection->log, 0,
                      "cache_tag index update failed for \"%V\"",
                      &r->cache->file.name);
    }
#endif

    return NGX_OK;
}


/*
 * Register the header filter and log phase handler.
 * Called from the postconfiguration slot in the module context.
 */
ngx_int_t
ngx_http_cache_pilot_index_postconfiguration(ngx_conf_t *cf) {
    ngx_http_handler_pt        *h;
    ngx_http_core_main_conf_t  *cmcf;

    ngx_http_next_cache_pilot_index_header_filter = ngx_http_top_header_filter;
    ngx_http_top_header_filter = ngx_http_cache_pilot_index_header_filter;

    cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

    h = ngx_array_push(&cmcf->phases[NGX_HTTP_LOG_PHASE].handlers);
    if (h == NULL) {
        return NGX_ERROR;
    }

    *h = ngx_http_cache_pilot_index_log_handler;

    return NGX_OK;
}
