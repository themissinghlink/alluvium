VENV_DIR=venv
VENV_BIN_DIR=${VENV_DIR}/bin
VENV_PYTHON=$(VENV_BIN_DIR)/python

BLACK_ARGS=--exclude=$(VENV_DIR) --line-length=100 .

default: install lint tests

venv:
	virtualenv --python=python3.7 $(VENV_DIR)

install: venv
		@$(VENV_RUN)pip install -r requirements.txt -e .[dev]

deps: install
		@$(VENV_RUN)pip-compile --annotate --no-header --no-index --output-file requirements.txt

clean-pyc:
	@find ./ -name "*.pyc" -exec rm -rf {} \;

clean: clean-pyc
	@rm -fr ${VENV_DIR}

format: isort black autopep8

autopep8:
	@$(VENV_RUN)autopep8 --in-place --recursive --exclude=${VENV_DIR} .

black:
	@$(VENV_RUN)black $(BLACK_ARGS)

isort:
	@$(VENV_RUN)isort $(ISORT_ARGS)

tests:
	# Run pytest with coverage
	@$(VENV_RUN)pytest --cov=. tests