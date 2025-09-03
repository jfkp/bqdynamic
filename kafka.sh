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

# Ports
BROKER_PLAINTEXT_PORT=9092
BROKER_SSL_PORT=9093
CONTROLLER_PORT=9094

# SSL Configuration
ENABLE_SSL=true  # true/false
CERT_PATH=/data/certs
CERT_FILENAME=broker-101.streaming.iaas.cib.gca.pem
KEY_FILENAME=broker-101.streaming.iaas.cib.gca.key
CERT_FILE="$CERT_PATH/$CERT_FILENAME"
KEY_FILE="$CERT_PATH/$KEY_FILENAME"
CERTS_DIR=$KAFKA_BASE/certs
SSL_PASSWORD=changeit

# Extract hostname from certificate filename
HOSTNAME=$(basename $CERT_FILE .pem)

# Start mode
USE_SYSTEMD=true  # true=systemctl, false=direct

# ==============================
# Clean Environment
# ==============================
clean_slate() {
    rm -Rf $KAFKA_BASE
    tar zxf /data/kafka_${KAFKA_SCALA}-${KAFKA_VERSION}.tgz -C $DATA_BASE
    rm -Rf /tmp/kraft-combined-logs/
}

# ==============================
# Initialize Kafka Storage
# ==============================
init() {
    cd $KAFKA_BASE
    KAFKA_UUID=$(kafka-storage.sh random-uuid)
    echo $KAFKA_UUID > myid
    mkdir -p logs
    kafka-storage.sh format -t $KAFKA_UUID -c $KAFKA_BASE/config/server.properties --standalone
}

# ==============================
# Enable SSL
# ==============================
enable_ssl() {
    echo "Configuring SSL..."
    mkdir -p $CERTS_DIR
    cp $CERT_FILE $CERTS_DIR/
    cp $KEY_FILE $CERTS_DIR/

    openssl pkcs12 -export \
        -in $CERTS_DIR/$(basename $CERT_FILE) \
        -inkey $CERTS_DIR/$(basename $KEY_FILE) \
        -name kafka \
        -out $CERTS_DIR/kafka.server.p12 \
        -password pass:$SSL_PASSWORD

    keytool -importkeystore \
        -deststorepass $SSL_PASSWORD -destkeypass $SSL_PASSWORD -destkeystore $CERTS_DIR/kafka.server.keystore.jks \
        -srckeystore $CERTS_DIR/kafka.server.p12 -srcstoretype PKCS12 -srcstorepass $SSL_PASSWORD \
        -alias kafka -noprompt

    keytool -import -trustcacerts -file $CERTS_DIR/$(basename $CERT_FILE) \
        -alias kafka -keystore $CERTS_DIR/kafka.server.truststore.jks \
        -storepass $SSL_PASSWORD -noprompt

    echo "SSL configured successfully in $CERTS_DIR"
}

# ==============================
# Generate Client SSL Config
# ==============================
generate_client_ssl_config() {
    CLIENT_CONFIG_FILE=$KAFKA_BASE/config/ssl.properties
    cat > $CLIENT_CONFIG_FILE <<EOF
bootstrap.servers=${HOSTNAME}:${BROKER_SSL_PORT}
security.protocol=SSL
ssl.keystore.location=$CERTS_DIR/kafka.server.keystore.jks
ssl.keystore.password=$SSL_PASSWORD
ssl.key.password=$SSL_PASSWORD
ssl.truststore.location=$CERTS_DIR/kafka.server.truststore.jks
ssl.truststore.password=$SSL_PASSWORD
EOF
    echo "Client SSL config generated at $CLIENT_CONFIG_FILE"
}

# ==============================
# Configure server.properties
# ==============================
configure_server() {
    LISTENERS="PLAINTEXT://:${BROKER_PLAINTEXT_PORT},SSL://:${BROKER_SSL_PORT},CONTROLLER://:${CONTROLLER_PORT}"
    ADVERTISED_LISTENERS="PLAINTEXT://${HOSTNAME}:${BROKER_PLAINTEXT_PORT},SSL://${HOSTNAME}:${BROKER_SSL_PORT},CONTROLLER://${HOSTNAME}:${CONTROLLER_PORT}"
    INTER_BROKER="PLAINTEXT"

    if [ "$ENABLE_SSL" = true ]; then
        SSL_CONFIG="
ssl.keystore.location=$CERTS_DIR/kafka.server.keystore.jks
ssl.keystore.password=$SSL_PASSWORD
ssl.key.password=$SSL_PASSWORD
ssl.truststore.location=$CERTS_DIR/kafka.server.truststore.jks
ssl.truststore.password=$SSL_PASSWORD
ssl.client.auth=required
"
    else
        SSL_CONFIG=""
    fi

    cat > $KAFKA_BASE/config/server.properties <<EOF
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=1@${HOSTNAME}:${CONTROLLER_PORT}

listeners=$LISTENERS
advertised.listeners=$ADVERTISED_LISTENERS
inter.broker.listener.name=$INTER_BROKER
controller.listener.names=CONTROLLER

listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL

num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

log.dirs=/tmp/kraft-combined-logs
num.partitions=1
num.recovery.threads.per.data.dir=1

offsets.topic.replication.factor=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

$SSL_CONFIG
EOF
    echo "server.properties configured for PLAINTEXT=${BROKER_PLAINTEXT_PORT}, SSL=${BROKER_SSL_PORT}, CONTROLLER=${CONTROLLER_PORT}"
}

# ==============================
# Wait until Kafka is ready
# ==============================
wait_for_kafka() {
    echo "Waiting for Kafka to become ready..."
    MAX_RETRIES=30
    RETRY_INTERVAL=5
    COUNT=0

    while true; do
        if [ "$ENABLE_SSL" = true ]; then
            kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_SSL_PORT} --list --command-config $KAFKA_BASE/config/ssl.properties &>/dev/null
        else
            kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_PLAINTEXT_PORT} --list &>/dev/null
        fi

        if [ $? -eq 0 ]; then
            echo "Kafka is ready!"
            break
        fi

        COUNT=$((COUNT + 1))
        if [ $COUNT -ge $MAX_RETRIES ]; then
            echo "Kafka did not become ready after $((MAX_RETRIES*RETRY_INTERVAL)) seconds."
            exit 1
        fi
        sleep $RETRY_INTERVAL
    done
}

# ==============================
# Topic Management
# ==============================
create_topics() {
    TOPIC_FILE=$PROJECT_BASE/topics
    if [ ! -f "$TOPIC_FILE" ]; then
        TOPICS=("default_topic")
    else
        mapfile -t TOPICS < <(grep -vE '^\s*($|#)' "$TOPIC_FILE")
    fi
    for topic in "${TOPICS[@]}"; do
        if [ "$ENABLE_SSL" = true ]; then
            kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_SSL_PORT} --create --topic "$topic" --command-config $KAFKA_BASE/config/ssl.properties
        else
            kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_PLAINTEXT_PORT} --create --topic "$topic"
        fi
    done
}

list_topics() {
    if [ "$ENABLE_SSL" = true ]; then
        kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_SSL_PORT} --list --command-config $KAFKA_BASE/config/ssl.properties
    else
        kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_PLAINTEXT_PORT} --list
    fi
}

# ==============================
# Generate Producer Script (auto SSL/PLAINTEXT)
# ==============================
generate_producer_script() {
    PRODUCER_SCRIPT=$KAFKA_BASE/run_producer.sh
    cat > $PRODUCER_SCRIPT <<'EOF'
#!/bin/bash
TOPIC=$1
if [ -z "$TOPIC" ]; then
  echo "Usage: ./run_producer.sh <topic>"
  exit 1
fi
EOF

    cat >> $PRODUCER_SCRIPT <<EOF
if [ "$ENABLE_SSL" = true ]; then
    kafka-console-producer.sh --bootstrap-server ${HOSTNAME}:${BROKER_SSL_PORT} --topic \$TOPIC --producer.config $KAFKA_BASE/config/ssl.properties
else
    kafka-console-producer.sh --bootstrap-server ${HOSTNAME}:${BROKER_PLAINTEXT_PORT} --topic \$TOPIC
fi
EOF

    chmod +x $PRODUCER_SCRIPT
}

# ==============================
# Generate Consumer Script (auto SSL/PLAINTEXT)
# ==============================
generate_consumer_script() {
    CONSUMER_SCRIPT=$KAFKA_BASE/run_consumer.sh
    cat > $CONSUMER_SCRIPT <<'EOF'
#!/bin/bash
TOPIC=$1
if [ -z "$TOPIC" ]; then
  echo "Usage: ./run_consumer.sh <topic>"
  exit 1
fi
EOF

    cat >> $CONSUMER_SCRIPT <<EOF
if [ "$ENABLE_SSL" = true ]; then
    kafka-console-consumer.sh --bootstrap-server ${HOSTNAME}:${BROKER_SSL_PORT} --topic \$TOPIC --consumer.config $KAFKA_BASE/config/ssl.properties --from-beginning
else
    kafka-console-consumer.sh --bootstrap-server ${HOSTNAME}:${BROKER_PLAINTEXT_PORT} --topic \$TOPIC --from-beginning
fi
EOF

    chmod +x $CONSUMER_SCRIPT
}

# ==============================
# Systemd Service
# ==============================
create_systemd_service() {
    SERVICE_FILE=/etc/systemd/system/kafka.service
    sudo tee $SERVICE_FILE > /dev/null <<EOF
[Unit]
Description=Apache Kafka Server
After=network.target

[Service]
Type=simple
User=$(whoami)
ExecStart=$KAFKA_BASE/bin/kafka-server-start.sh $KAFKA_BASE/config/server.properties
ExecStop=$KAFKA_BASE/bin/kafka-server-stop.sh
Restart=on-abnormal
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable kafka
}

# ==============================
# Start Kafka
# ==============================
start_kafka() {
    if [ "$USE_SYSTEMD" = true ]; then
        create_systemd_service
        sudo systemctl start kafka
    else
        kafka-server-start.sh -daemon $KAFKA_BASE/config/server.properties
    fi
}


# ==============================
# Cleanup / Uninstall Kafka
# ==============================
cleanup_kafka() {
    echo "Stopping Kafka..."
    if [ "$USE_SYSTEMD" = true ]; then
        sudo systemctl stop kafka
        sudo systemctl disable kafka
        sudo rm -f /etc/systemd/system/kafka.service
        sudo systemctl daemon-reload
    else
        kafka-server-stop.sh
    fi

    echo "Removing Kafka installation..."
    rm -rf $KAFKA_BASE
    rm -rf /tmp/kraft-combined-logs

    if [ "$ENABLE_SSL" = true ]; then
        echo "Removing SSL certificates..."
        rm -rf $CERTS_DIR
    fi

    echo "Kafka cleanup complete!"
}

# ==============================
# Phases
# ==============================
phase1() {
    clean_slate
    init
    if [ "$ENABLE_SSL" = true ]; then
        enable_ssl
        generate_client_ssl_config
    fi
    configure_server
    generate_producer_script
    generate_consumer_script
}

phase2() {
    start_kafka
}

phase3() {
    wait_for_kafka
    create_topics
    list_topics
}

# ==============================
# Execute requested function
# ==============================
"$@"

./kafka_script.sh phase1
./kafka_script.sh phase2
./kafka_script.sh phase3
$KAFKA_BASE/run_producer.sh my_topic
$KAFKA_BASE/run_consumer.sh my_topic
./kafka_script.sh cleanup_kafka


