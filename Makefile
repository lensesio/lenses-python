.PHONY: clean distclean

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
	python3 setup.py clean

decrypt_license:
	gpg --quiet --batch --yes --decrypt --passphrase="DECRYPT_PSK" \
		--output ./license.json license.json.gpg

rflake:
	PATH="${ENV}":"${PATH}"
	flake8 lensesio/

docker:
	@if docker ps -a | grep -q lenses-box; then \
		if docker inspect -f '{{.State.Running}}' lenses-box | grep -iq "true"; then \
			echo "Lenses box is already running!"; \
			echo "Shutting down lenses-box"; \
			docker stop lenses-box && sleep 10; \
		else \
			docker rm lenses-box; \
		fi; \
	fi

	@docker run \
	-e EULA="https://dl.lenses.stream/d/?id=$(LICENSE_KEY)" \
	--rm -d \
	-e ENABLE_SSL=1 \
	--env-file _resources/acls-dev.env \
	-p 3030:3030 -p 9093:9093 -p 9092:9092 -p 2181:2181 -p 8081:8081 -p 9581:9581 -p 9582:9582 -p 9584:9584 -p 9585:9585 \
	-v "${PWD}"/license.json:/run/lenses/license.conf \
	--name=lenses-box \
	lensesio/box:latest

install: requirements-dev.txt setup.py
	[ ! -d "$(ENV)/" ] && python3 -m venv $(ENV)/ || :
	python3 setup.py install
	pip install --exists-action w $(REQUIREMENTS)

pep8: install
	$(FLAKE8) --statistics ./$(PROJECT)/ setup.py

test: .wait-lenses
	$(TOX)

.wait-lenses:
	@echo "WAITING LENSES..."
	./tests/wait-for-lenses-box.sh
