.PHONY: all
.SILENT:

build-docker:
	docker build -t spotify-web-api:local .

install:
	pip install -e .

run:
	flask run

run-uwsgi:
	uwsgi --http 127.0.0.1:8001 --master -p 1 -w api:app
