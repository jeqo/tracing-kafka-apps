MVN := ./mvnw

.PHONY: all
all: build docker-base-up docker-image topics docker-apps-up

.PHONY: build
build:
	${MVN} clean install

.PHONY: docker-image
docker-image:
	${MVN} jib:dockerBuild

.PHONY: topics
topics:
	kafka-topics --zookeeper localhost:2181 --create --topic events-v1 --partitions 1 --replication-factor 1 --if-not-exists
	kafka-topics --zookeeper localhost:2181 --create --topic enriched-events-v1 --partitions 1 --replication-factor 1 --if-not-exists
	kafka-topics --zookeeper localhost:2181 --create --topic metadata-v1 --partitions 1 --replication-factor 1 --if-not-exists

.PHONY: docker-base-up
docker-base-up:
	docker-compose up -d

.PHONY: docker-apps-up
docker-apps-up:
	docker-compose -f docker-compose.yml -f docker-compose.apps.yml up -d

.PHONY: docker-destroy
docker-destroy:
	docker-compose down --remove-orphans

.PHONY: perf-test-small
perf-test-small:
	curl http://localhost:8081
	wrk -c 10 -t 4 -d 5 http://localhost:8080