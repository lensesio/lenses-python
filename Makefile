.PHONY: clean

# Project settings
PROJECT = lensesio

# Virtual environment settings
ENV ?= venv
IS_VENV = $(shell python -c "import sys; print(int(hasattr(sys, 'real_prefix')));")

# Determine if Python is running inside virtualenv
ifeq ($(IS_VENV),1)
	FLAKE8 = flake8
	TOX = tox
else
	FLAKE8 = $(ENV)/bin/flake8
	TOX = $(ENV)/bin/tox
endif

# Directories
DIST_DIR = ./dist
TOX_DIR = ./.tox
CLEAN_DIRS = ./$(PROJECT) $(ENV) $(shell [ -d $(TOX_DIR) ] && echo $(TOX_DIR) || :)
REQUIREMENTS = -r requirements-dev.txt

all: install

clean:
	@python3 setup.py clean

decrypt_license:
	@if [ ! -e _resources/lenses-kerberos/license.json  ]; then \
		echo "Decrypting license..."; \
		gpg --quiet --batch --yes --decrypt --passphrase="${DECRYPT_PSK}" \
			--output _resources/lenses-kerberos/license.json license.json.gpg; \
	fi

docker: decrypt_license
	@docker-compose -f _resources/lenses-kerberos/kerberos.yaml build
	@docker-compose -f _resources/lenses-kerberos/kerberos.yaml down
	@rm -rf _resources/lenses-kerberos/local
	@mkdir -vp _resources/lenses-kerberos/local
	@docker-compose -f _resources/lenses-kerberos/kerberos.yaml up -d

	@if [ ! -e _resources/lenses-kerberos/license.json ]; then \
		echo "Licese is missing."; \
		exit 1; \
	fi
	@echo waiting 20 seconds before starting lenses
	@sleep 20
	@if docker ps -a | grep -q lenses-box; then \
		echo "Lenses box is already running!"; \
		echo "Shutting down lenses-box"; \
		docker-compose -f _resources/lenses-kerberos/lenses-box.yaml down && sleep 10; \
	fi

	@docker-compose -f _resources/lenses-kerberos/lenses-box.yaml up -d

docker_clean:
	@docker-compose -f _resources/lenses-kerberos/kerberos.yaml down
	@docker-compose -f _resources/lenses-kerberos/lenses-box.yaml down
	@rm -rf _resources/lenses-kerberos/local

build_py:
	@python3 setup.py sdist bdist_wheel

install:
	@if [ -e dist/lensesio-4.0.1-py3-none-any.whl ]; then \
		pip install dist/lensesio-4.0.1-py3-none-any.whl; \
	else \
		echo "No dist package found, please run make build_py first, to build the package"; \
	fi

test_install: virtenv
	@./venv/bin/pip3 install -U setuptools
	@./venv/bin/python3 setup.py sdist bdist_wheel
	@./venv/bin/pip3 install -U dist/lensesio-4.0.1-py3-none-any.whl

mkvirtenv:
	@if [ ! -e venv ]; then \
		python3 -m virtualenv -p $(shell which python3) venv; \
		. venv/bin/activate ;\
		./venv/bin/pip3 install -Ur requirements-dev.txt ; \
	fi

rmvirtenv:
	@if command -v deactivate; then \
		deactivate; \
	fi

	@rm -rf venv

virtenv: mkvirtenv
	@. venv/bin/activate ;
	@pip -V

pep8: virtenv
	@$(FLAKE8) --statistics ./$(PROJECT)/ setup.py

test: virtenv .wait-lenses
	@${TOX}

.wait-lenses:
	@echo "WAITING LENSES..."
	@./tests/wait-for-lenses-box.sh 30
