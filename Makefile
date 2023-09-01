VENV = venv
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip

add_src_to_pypath:
	export PYTHONPATH=$$PYTHONPATH:$(pwd)/src

test:
	python3 -m pytest -s

lint: add_src_to_pypath
	pylint src

freeze:
	$(PIP) freeze > requirements.txt

dependencies:
	$(PIP) install -r requirements.txt

clean:
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete