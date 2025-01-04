.PHONY: all
.SILENT:
ARGS = `arg="$(filter-out $@,$(MAKECMDGOALS))" && echo $${arg:-${1}}`
%:
	@:

build-airflow:
	docker build -t airflow:local -f containers/airflow/Dockerfile .

build-spark:
	docker build -t spark:local -f containers/spark/Dockerfile .

clean:
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf *.log
	find . -name '.pytest_cache' -delete
	find . -name '*.pyc' -delete

clear-cache:
	rm -rf .cache-*

deps: requirements.txt
	echo "Installing dependencies..."
	pip install -r requirements.txt

install:
	pip install -e .

test:
	pytest -v -s tests