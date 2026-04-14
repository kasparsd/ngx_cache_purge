NGINX_VERSION ?= 1.25.5
NGINX_SRC_DIR ?= /opt/nginx-src/nginx-$(NGINX_VERSION)
NGINX_BUILD_PREFIX ?= /opt/nginx
MODULE_DIR ?= /workspace
JOBS ?= $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)

.PHONY: help nginx-build nginx-version format test

help:
	@printf '%s\n' \
		'make nginx-build      Build NGINX with this module' \
		'make nginx-version    Build info for the installed NGINX binary' \
		'make format           Run the repository formatter' \
		'make test             Run the Test::Nginx suite'

nginx-build:
	test -d "$(NGINX_SRC_DIR)"
	cd "$(NGINX_SRC_DIR)" && ./configure \
		--prefix="$(NGINX_BUILD_PREFIX)" \
		--with-debug \
		--with-http_ssl_module \
		--add-module="$(MODULE_DIR)"
	$(MAKE) -C "$(NGINX_SRC_DIR)" -j"$(JOBS)"
	$(MAKE) -C "$(NGINX_SRC_DIR)" install

nginx-version:
	"$(NGINX_BUILD_PREFIX)/sbin/nginx" -V

format:
	astyle -v --options='.astylerc' ngx_cache_purge_module.c || (echo 'astyle failed'; exit 1)
	dos2unix --quiet ngx_cache_purge_module.c || (echo 'dos2unix failed'; exit 2)

test:
	$(MAKE) nginx-build >/tmp/nginx-build.log
	TEST_NGINX_BINARY="$(NGINX_BUILD_PREFIX)/sbin/nginx" prove t
