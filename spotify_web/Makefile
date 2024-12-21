.PHONY: all
.SILENT:

build-docker:
	docker build -t spotify-web-api:local .

install:
	pip install -e .

run:
	flask run

run-uwsgi:
	uwsgi --http 127.0.0.1:8000 --master -p 2 -w api:app
