#!/bin/bash
set -x

DATA_BASE=/data
KAFKA_BASE=$DATA_BASE/kafka_2.13-4.0.0

PATH=$PATH:$KAFKA_BASE/bin

PROJECT_BASE=$(dirname $(dirname $( realpath $0)))

CERTS_DIR=$KAFKA_BASE/certs
SSL_PASSWORD=changeit

clean_slate() {
    rm -Rf /data/kafka_2.13-4.0.0
    tar zxf /data/kafka_2.13-4.0.0.tgz
    rm -Rf /tmp/kraft-combined-logs/
}

init() {
    cd $KAFKA_BASE

    KAFKA_UUID=$( kafka-storage.sh random-uuid )
    echo $KAFKA_UUID >myid

    mkdir -p logs

    kafka-storage.sh format -t $KAFKA_UUID -c $KAFKA_BASE/config/server.properties --standalone
}

enable_ssl() {
    echo "Configuring SSL..."
    mkdir -p $CERTS_DIR

    # Generate a self-signed keystore
    keytool -genkey -noprompt \
        -alias kafka \
        -dname "CN=localhost, OU=IT, O=MyOrg, L=City, S=State, C=FR" \
        -keystore $CERTS_DIR/kafka.server.keystore.jks \
        -keyalg RSA -storepass $SSL_PASSWORD -keypass $SSL_PASSWORD

    # Export cert and import into truststore
    keytool -export -alias kafka -file $CERTS_DIR/kafka-cert -keystore $CERTS_DIR/kafka.server.keystore.jks -storepass $SSL_PASSWORD
    keytool -import -alias kafka -file $CERTS_DIR/kafka-cert -keystore $CERTS_DIR/kafka.server.truststore.jks -storepass $SSL_PASSWORD -noprompt || true

    # Update server.properties with SSL configuration
    cat >> $KAFKA_BASE/config/server.properties <<EOF

# SSL configuration
listeners=SSL://:9092
advertised.listeners=SSL://localhost:9092
listener.security.protocol.map=SSL:SSL
inter.broker.listener.name=SSL

ssl.keystore.location=$CERTS_DIR/kafka.server.keystore.jks
ssl.keystore.password=$SSL_PASSWORD
ssl.key.password=$SSL_PASSWORD
ssl.truststore.location=$CERTS_DIR/kafka.server.truststore.jks
ssl.truststore.password=$SSL_PASSWORD
ssl.client.auth=required
EOF

    echo "SSL configuration enabled."
}

create_topics() {
    for topic in $( cat $PROJECT_BASE/topics ) ; do
	kafka-topics.sh  --bootstrap-server localhost:9092  --create --topic $topic
    done
}

list_topics() {
    kafka-topics.sh  --bootstrap-server localhost:9092  --list
}

phase1() {
    clean_slate
    init
    enable_ssl
}

phase2() {
    echo 'Starting Kafka with SSL...'
    kafka-server-start.sh -daemon $KAFKA_BASE/config/server.properties
}

phase3() {
    create_topics
    list_topics
}

"$@"
