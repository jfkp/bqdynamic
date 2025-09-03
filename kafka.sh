#!/bin/bash
set -x

# ==============================
# Variables
# ==============================
DATA_BASE=/data
KAFKA_VERSION=4.0.0
KAFKA_SCALA=2.13
KAFKA_BASE=$DATA_BASE/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}
PATH=$PATH:$KAFKA_BASE/bin
PROJECT_BASE=$(dirname $(dirname $(realpath $0)))

# SSL certificate & key variables (update paths)
CERT_FILE=/path/to/broker-101.streaming.iaas.cib.gca.pem
KEY_FILE=/path/to/broker-101.streaming.iaas.cib.gca.key
CERTS_DIR=$KAFKA_BASE/certs
SSL_PASSWORD=changeit

# ==============================
# Clean Environment
# ==============================
clean_state() {
    rm -Rf $KAFKA_BASE
    tar zxf /data/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz -C $DATA_BASE
    rm -Rf /tmp/kraft-combined-logs/
}

# ==============================
# Initialize Kafka Storage (KRaft)
# ==============================
init() {
    cd $KAFKA_BASE
    KAFKA_UUID=$(kafka-storage.sh random-uuid)
    echo $KAFKA_UUID > myid
    mkdir -p logs

    kafka-storage.sh format -t $KAFKA_UUID -c $KAFKA_BASE/config/server.properties --standalone
}

# ==============================
# Enable SSL using existing cert/key
# ==============================
enable_ssl() {
    echo "Configuring SSL using provided certificate and key..."
    mkdir -p $CERTS_DIR

    # Copy cert and key into Kafka certs directory
    cp $CERT_FILE $CERTS_DIR/
    cp $KEY_FILE $CERTS_DIR/

    # Convert PEM + KEY -> PKCS12 keystore
    openssl pkcs12 -export \
        -in $CERTS_DIR/$(basename $CERT_FILE) \
        -inkey $CERTS_DIR/$(basename $KEY_FILE) \
        -name kafka \
        -out $CERTS_DIR/kafka.server.p12 \
        -password pass:$SSL_PASSWORD

    # Import PKCS12 -> Java keystore
    keytool -importkeystore \
        -deststorepass $SSL_PASSWORD -destkeypass $SSL_PASSWORD -destkeystore $CERTS_DIR/kafka.server.keystore.jks \
        -srckeystore $CERTS_DIR/kafka.server.p12 -srcstoretype PKCS12 -srcstorepass $SSL_PASSWORD \
        -alias kafka -noprompt

    # Create truststore from certificate
    keytool -import -trustcacerts -file $CERTS_DIR/$(basename $CERT_FILE) \
        -alias kafka -keystore $CERTS_DIR/kafka.server.truststore.jks \
        -storepass $SSL_PASSWORD -noprompt

    echo "SSL configured successfully in $CERTS_DIR"
}

# ==============================
# Configure server.properties for KRaft + SSL
# ==============================
configure_server() {
    cat > $KAFKA_BASE/config/server.properties <<EOF
############################# Server Basics #############################
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=1@localhost:9093

############################# Socket Server Settings #############################
listeners=SSL://:9092,CONTROLLER://:9093
advertised.listeners=SSL://localhost:9092,CONTROLLER://:9093
inter.broker.listener.name=SSL
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL

############################# SSL Settings #############################
ssl.keystore.location=$CERTS_DIR/kafka.server.keystore.jks
ssl.keystore.password=$SSL_PASSWORD
ssl.key.password=$SSL_PASSWORD
ssl.truststore.location=$CERTS_DIR/kafka.server.truststore.jks
ssl.truststore.password=$SSL_PASSWORD
ssl.client.auth=required

############################# Threading #############################
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

############################# Log Basics #############################
log.dirs=/tmp/kraft-combined-logs
num.partitions=1
num.recovery.threads.per.data.dir=1

############################# Internal Topic Settings #############################
offsets.topic.replication.factor=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

############################# Log Retention #############################
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
EOF
    echo "server.properties configured for KRaft + SSL"
}

# ==============================
# Topic Management
# ==============================
create_topics() {
    for topic in $(cat $PROJECT_BASE/topics); do
        kafka-topics.sh --bootstrap-server localhost:9092 --create --topic $topic
    done
}

list_topics() {
    kafka-topics.sh --bootstrap-server localhost:9092 --list
}

# ==============================
# Phases
# ==============================
phase1() {
    clean_state
    init
    enable_ssl
    configure_server
}

phase2() {
    echo 'Starting Kafka with SSL...'
    kafka-server-start.sh -daemon $KAFKA_BASE/config/server.properties
}

phase3() {
    create_topics
    list_topics
}

# ==============================
# Execute specified function
# ==============================
"$@"
