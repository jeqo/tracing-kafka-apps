MVN := ./mvnw

.PHONY: all
all: build docker-base-up docker-image topics docker-apps-up

.PHONY: format
format:
	${MVN} spring-javaformat:apply

.PHONY: build
build:
	${MVN} clean install

.PHONY: docker-image
docker-image:
	${MVN} jib:dockerBuild

.PHONY: topics
topics:
	docker-compose exec kafka kafka-topics --zookeeper zookeeper:2181 --create --topic events-v1 --partitions 1 --replication-factor 1 --if-not-exists
	docker-compose exec kafka kafka-topics --zookeeper zookeeper:2181 --create --topic enriched-events-v1 --partitions 1 --replication-factor 1 --if-not-exists
	docker-compose exec kafka kafka-topics --zookeeper zookeeper:2181 --create --topic metadata-v1 --partitions 1 --replication-factor 1 --if-not-exists

.PHONY: docker-base-up
docker-base-up:
	docker-compose up -d

.PHONY: docker-apps-up
docker-apps-up:
	docker-compose -f docker-compose.yml -f docker-compose.apps.yml up -d

.PHONY: docker-destroy
docker-destroy:
	docker-compose down --remove-orphans

.PHONY: perf-test
perf-test:
	curl http://localhost:18080
	wrk -c 3 -t 3 -d 3 http://localhost:8080

CCLOUD_CLUSTER_ID = lkc-9pkgm
ccloud-use-cluster:
	ccloud kafka cluster use ${CCLOUD_CLUSTER_ID}

ccloud-service-account-metadata: ccloud-use-cluster
	ccloud service-account create kafka-app-metadata --description "Testing applications: Metadata"
ccloud-service-account-events: ccloud-use-cluster
	ccloud service-account create kafka-app-events --description "Testing applications: Events"
ccloud-service-account-joiner: ccloud-use-cluster
	ccloud service-account create kafka-app-joiner --description "Testing applications: Joiner"
ccloud-service-account-console: ccloud-use-cluster
	ccloud service-account create kafka-app-console --description "Testing applications: Console"

CCLOUD_SA_METADATA_ID = 88782
ccloud-acls-events: ccloud-use-cluster
	ccloud kafka acl create --allow --service-account "${CCLOUD_SA_EVENTS_ID}" --operation "WRITE" --operation "DESCRIBE" --topic "events-v1"

CCLOUD_SA_EVENTS_ID = 88783
ccloud-acls-metadata: ccloud-use-cluster
	ccloud kafka acl create --allow --service-account "${CCLOUD_SA_METADATA_ID}" --operation "WRITE" --operation "DESCRIBE" --topic "metadata-v1"

CCLOUD_SA_JOINER_ID = 88784
ccloud-acls-joiner: ccloud-use-cluster
	ccloud kafka acl create --allow --service-account "${CCLOUD_SA_JOINER_ID}" --operation "READ" --operation "DESCRIBE" --topic "events-v1"
	ccloud kafka acl create --allow --service-account "${CCLOUD_SA_JOINER_ID}" --operation "READ" --operation "DESCRIBE" --topic "metadata-v1"
	ccloud kafka acl create --allow --service-account "${CCLOUD_SA_JOINER_ID}" --operation "READ" --consumer-group "joiner-v1"
	ccloud kafka acl create --allow --service-account "${CCLOUD_SA_JOINER_ID}" --operation "WRITE" --operation "DESCRIBE" --topic "joined-v1"
	ccloud kafka acl create --allow --service-account "${CCLOUD_SA_JOINER_ID}" --operation "WRITE" --operation "DESCRIBE" --operation "READ" --operation "CREATE" --topic "joiner-v1" --prefix
CCLOUD_SA_CONSOLE_ID = 88785
ccloud-acls-console: ccloud-use-cluster
	ccloud kafka acl create --allow --service-account "${CCLOUD_SA_CONSOLE_ID}" --operation "READ" --operation "DESCRIBE" --topic "joined-v1"
	ccloud kafka acl create --allow --service-account "${CCLOUD_SA_CONSOLE_ID}" --operation "READ" --consumer-group "console-v1"
ccloud-acls: ccloud-acls-metadata ccloud-acls-events ccloud-acls-joiner ccloud-acls-console
ccloud-api-key:
	ccloud api-key create --service-account ${CCLOUD_SA_METADATA_ID} --resource ${CCLOUD_CLUSTER_ID}
	ccloud api-key create --service-account ${CCLOUD_SA_EVENTS_ID} --resource ${CCLOUD_CLUSTER_ID}
	ccloud api-key create --service-account ${CCLOUD_SA_JOINER_ID} --resource ${CCLOUD_CLUSTER_ID}
	ccloud api-key create --service-account ${CCLOUD_SA_CONSOLE_ID} --resource ${CCLOUD_CLUSTER_ID}
