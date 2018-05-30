# Makefile that builds an image strictly for testing, and also runs those tests
# There is no production image to be published

.PHONY: build test clean clean-files clean-docker debug rebuild docker-compose

docker-compose :
	# Download docker-compose executable for use on Jenkins
	# See https://docs.docker.com/compose/install/ for installing docker compose
	curl -L --fail "https://github.com/docker/compose/releases/download/1.14.0/docker-compose-`uname -s`-`uname -m`" -o docker-compose
	chmod +x docker-compose

build :
	@echo "****** BUILDING ******"
	docker-compose build --force-rm

rebuild : clean
	@echo "****** REBUILDING ******"
	docker-compose build --force-rm --no-cache --pull

test : build
	@echo "****** RUNNING TESTS ******"
	docker-compose up --exit-code-from tests --remove-orphans --force-recreate

debug : 
	@echo "****** OPENING DEBUG SHELL ******"
	docker-compose run --rm test bash

clean : clean-files clean-docker

clean-files :
	@echo "****** CLEANING FILES ******"
	rm -rf .cache/
	rm -rf .pytest_cache/
	find * -name "__pycache__" | xargs rm -rf
	find * -name "*.pyc" | xargs rm -f

clean-docker :
	@echo "****** CLEANING DOCKER ******"
	docker-compose down --rmi all
	docker-compose rm -f
