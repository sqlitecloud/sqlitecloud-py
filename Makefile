VENV = venv
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip

add_src_to_pypath:
	export PYTHONPATH=$$PYTHONPATH:$(pwd)/src

test:
	export PYTHONPATH=$$PYTHONPATH:$(pwd)/src
	python3 -m pytest -s ./src

lint: add_src_to_pypath
	pylint src

freeze:
	$(PIP) freeze > requirements.txt

dependencies:
	$(PIP) install -r requirements.txt

clean:
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete