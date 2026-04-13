NGINX_VERSION ?= 1.25.5
NGINX_SRC_DIR ?= /opt/nginx-src/nginx-$(NGINX_VERSION)
NGINX_BUILD_PREFIX ?= /opt/nginx
MODULE_DIR ?= /workspace
JOBS ?= $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)
DOCKER_COMPOSE ?= docker compose
DOCKER_SERVICE ?= dev
IN_CONTAINER ?= $(shell [ -f /.dockerenv ] && echo 1 || echo 0)

.PHONY: help image shell nginx-build nginx-version test \
	_docker-run _nginx-build _nginx-version _test

help:
	@printf '%s\n' \
		'make image            Build the development image' \
		'make shell            Open a shell in the development container' \
		'make nginx-build      Build NGINX with this module' \
		'make nginx-version    Build info for the installed NGINX binary' \
		'make test             Run the Test::Nginx suite'

image:
	$(DOCKER_COMPOSE) build

shell:
	$(DOCKER_COMPOSE) run --rm $(DOCKER_SERVICE)

_docker-run:
	$(DOCKER_COMPOSE) run --rm $(DOCKER_SERVICE) make $(TARGET)

nginx-build:
ifeq ($(IN_CONTAINER),1)
	$(MAKE) _nginx-build
else
	$(MAKE) TARGET=_nginx-build _docker-run
endif

_nginx-build:
	test -d "$(NGINX_SRC_DIR)"
	cd "$(NGINX_SRC_DIR)" && ./configure \
		--prefix="$(NGINX_BUILD_PREFIX)" \
		--with-debug \
		--with-http_ssl_module \
		--add-module="$(MODULE_DIR)"
	$(MAKE) -C "$(NGINX_SRC_DIR)" -j"$(JOBS)"
	$(MAKE) -C "$(NGINX_SRC_DIR)" install

nginx-version:
ifeq ($(IN_CONTAINER),1)
	$(MAKE) _nginx-version
else
	$(DOCKER_COMPOSE) run --rm $(DOCKER_SERVICE) sh -lc 'make _nginx-build >/tmp/nginx-build.log && make _nginx-version'
endif

_nginx-version:
	"$(NGINX_BUILD_PREFIX)/sbin/nginx" -V

test:
ifeq ($(IN_CONTAINER),1)
	$(MAKE) _test
else
	$(MAKE) TARGET=_test _docker-run
endif

_test:
	$(MAKE) _nginx-build >/tmp/nginx-build.log
	TEST_NGINX_BINARY="$(NGINX_BUILD_PREFIX)/sbin/nginx" prove t
