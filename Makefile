VENV = venv
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip

test:
	python3 -m pytest -s

lint:
	pylint src

freeze:
	$(PIP) freeze > requirements.txt

dependencies:
	$(PIP) install -r requirements.txt

clean:
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete