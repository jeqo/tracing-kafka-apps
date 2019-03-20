MVN := ./mvnw

.PHONY: all
all:

.PHONY: build
build:
	${MVN} clean compile

.PHONY: docker-image
docker-image: build
	${MVN} jib:dockerBuild