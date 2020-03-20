IMAGE="builtin/simple-data-dictionary"
SSH_PRIVATE_KEY="$$(cat "${BUILTIN_SSH_PRIVATE_KEY}")"
DOCKER_RUN=docker run -v $$(pwd)/csvs:/builtin/csvs --env-file .env -e SNOWFLAKE_USER=${SNOWFLAKE_USER} -e SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD} -e SLACK_TOKEN=${SLACK_TOKEN} $(IMAGE)

lint-test: build
	$(DOCKER_RUN) bin/lint-test

deploy: docker-login build tag push

build: requirements.txt requirements-dev.txt
	docker build --build-arg SSH_PRIVATE_KEY=$(SSH_PRIVATE_KEY) -t $(IMAGE) .

tag:
	docker tag $(IMAGE) ${AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com/$(IMAGE):${TAG}

push:
	docker push ${AWS_ACCOUNT_ID}.dkr.ecr.us-west-2.amazonaws.com/$(IMAGE):${TAG}

docker-login:
	$$(aws ecr get-login --no-include-email --registry-ids 763104351884)
	$$(aws ecr get-login --no-include-email)


lint: build
	$(DOCKER_RUN) bin/lint

test: build
	$(DOCKER_RUN) bin/test

run-fetch: build
	$(DOCKER_RUN) fetch 

run-pipe: build
	$(DOCKER_RUN) pipe

run-cluster: build
	$(DOCKER_RUN) cluster

requirements.txt: Pipfile Pipfile.lock
	pipenv lock --requirements > requirements.txt

requirements-dev.txt: Pipfile Pipfile.lock
	pipenv lock --dev --requirements > requirements-dev.txt

.PHONY: deploy build tag push docker-login lint test lint-test
