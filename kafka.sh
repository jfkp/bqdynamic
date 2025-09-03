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

# Kafka ports
BROKER_PORT=9092
CONTROLLER_PORT=9093

# SSL Configuration
ENABLE_SSL=true  # Set to "false" to run PLAINTEXT

# Base directory where your cert and key are located
CERT_PATH=/data/certs  # <-- set this to your cert folder

# Certificate and key filenames
CERT_FILENAME=broker-101.streaming.iaas.cib.gca.pem
KEY_FILENAME=broker-101.streaming.iaas.cib.gca.key

# Full paths
CERT_FILE="$CERT_PATH/$CERT_FILENAME"
KEY_FILE="$CERT_PATH/$KEY_FILENAME"

# Kafka SSL configs
CERTS_DIR=$KAFKA_BASE/certs
SSL_PASSWORD=changeit

# Extract hostname from certificate filename (without extension)
HOSTNAME=$(basename $CERT_FILE .pem)

# Flag to choose startup method
USE_SYSTEMD=true  # Set to false to start Kafka directly without systemd

# ==============================
# Clean Environment
# ==============================
clean_slate() {
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
# Generate client SSL config for producers/consumers
# ==============================
generate_client_ssl_config() {
    CLIENT_CONFIG_FILE=$KAFKA_BASE/config/ssl.properties
    cat > $CLIENT_CONFIG_FILE <<EOF
bootstrap.servers=${HOSTNAME}:${BROKER_PORT}
security.protocol=SSL
ssl.keystore.location=$CERTS_DIR/kafka.server.keystore.jks
ssl.keystore.password=$SSL_PASSWORD
ssl.key.password=$SSL_PASSWORD
ssl.truststore.location=$CERTS_DIR/kafka.server.truststore.jks
ssl.truststore.password=$SSL_PASSWORD
EOF

    echo "Client SSL configuration generated at $CLIENT_CONFIG_FILE with bootstrap server ${HOSTNAME}:${BROKER_PORT}"
}

# ==============================
# Configure server.properties for KRaft + SSL/PLAINTEXT
# ==============================
configure_server() {
    if [ "$ENABLE_SSL" = true ]; then
        LISTENERS="SSL://:${BROKER_PORT},CONTROLLER://:${CONTROLLER_PORT}"
        ADVERTISED_LISTENERS="SSL://${HOSTNAME}:${BROKER_PORT},CONTROLLER://${HOSTNAME}:${CONTROLLER_PORT}"
        INTER_BROKER="SSL"
        SSL_CONFIG="
ssl.keystore.location=$CERTS_DIR/kafka.server.keystore.jks
ssl.keystore.password=$SSL_PASSWORD
ssl.key.password=$SSL_PASSWORD
ssl.truststore.location=$CERTS_DIR/kafka.server.truststore.jks
ssl.truststore.password=$SSL_PASSWORD
ssl.client.auth=required
"
    else
        LISTENERS="PLAINTEXT://:${BROKER_PORT},CONTROLLER://:${CONTROLLER_PORT}"
        ADVERTISED_LISTENERS="PLAINTEXT://${HOSTNAME}:${BROKER_PORT},CONTROLLER://${HOSTNAME}:${CONTROLLER_PORT}"
        INTER_BROKER="PLAINTEXT"
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
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL

$SSL_CONFIG

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
EOF

    echo "server.properties configured with SSL=$ENABLE_SSL using host ${HOSTNAME}"
}

# ==============================
# Wait until Kafka is ready
# ==============================
wait_for_kafka() {
    echo "Waiting for Kafka to become ready at ${HOSTNAME}:${BROKER_PORT}..."
    MAX_RETRIES=30
    RETRY_INTERVAL=5
    COUNT=0

    while true; do
        if [ "$ENABLE_SSL" = true ]; then
            kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_PORT} --list --command-config $KAFKA_BASE/config/ssl.properties &>/dev/null
        else
            kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_PORT} --list &>/dev/null
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

        echo "Kafka not ready yet. Retrying in $RETRY_INTERVAL seconds..."
        sleep $RETRY_INTERVAL
    done
}

# ==============================
# Topic Management
# ==============================
create_topics() {
    TOPIC_FILE=$PROJECT_BASE/topics

    if [ ! -f "$TOPIC_FILE" ]; then
        echo "Topics file not found at $TOPIC_FILE. Creating a default topic 'default_topic'."
        TOPICS=("default_topic")
    else
        mapfile -t TOPICS < <(grep -vE '^\s*($|#)' "$TOPIC_FILE")
    fi

    for topic in "${TOPICS[@]}"; do
        if [ "$ENABLE_SSL" = true ]; then
            kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_PORT} --create --topic "$topic" --command-config $KAFKA_BASE/config/ssl.properties
        else
            kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_PORT} --create --topic "$topic"
        fi
        echo "Created topic: $topic"
    done
}

list_topics() {
    if [ "$ENABLE_SSL" = true ]; then
        kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_PORT} --list --command-config $KAFKA_BASE/config/ssl.properties
    else
        kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_PORT} --list
    fi
}

# ==============================
# Generate producer script
# ==============================
generate_producer_script() {
    PRODUCER_SCRIPT=$KAFKA_BASE/run_producer.sh
    cat > $PRODUCER_SCRIPT <<EOF
#!/bin/bash
TOPIC=\$1
if [ -z "\$TOPIC" ]; then
  echo "Usage: ./run_producer.sh <topic>"
  exit 1
fi
EOF

    if [ "$ENABLE_SSL" = true ]; then
        cat >> $PRODUCER_SCRIPT <<EOF
kafka-console-producer.sh \\
  --bootstrap-server ${HOSTNAME}:${BROKER_PORT} \\
  --topic \$TOPIC \\
  --producer.config $KAFKA_BASE/config/ssl.properties
EOF
    else
        cat >> $PRODUCER_SCRIPT <<EOF
kafka-console-producer.sh \\
  --bootstrap-server ${HOSTNAME}:${BROKER_PORT} \\
  --topic \$TOPIC
EOF
    fi

    chmod +x $PRODUCER_SCRIPT
    echo "Producer script generated at $PRODUCER_SCRIPT"
}

# ==============================
# Generate consumer script
# ==============================
generate_consumer_script() {
    CONSUMER_SCRIPT=$KAFKA_BASE/run_consumer.sh
    cat > $CONSUMER_SCRIPT <<EOF
#!/bin/bash
TOPIC=\$1
if [ -z "\$TOPIC" ]; then
  echo "Usage: ./run_consumer.sh <topic>"
  exit 1
fi
EOF

    if [ "$ENABLE_SSL" = true ]; then
        cat >> $CONSUMER_SCRIPT <<EOF
kafka-console-consumer.sh \\
  --bootstrap-server ${HOSTNAME}:${BROKER_PORT} \\
  --topic \$TOPIC \\
  --consumer.config $KAFKA_BASE/config/ssl.properties \\
  --from-beginning
EOF
    else
        cat >> $CONSUMER_SCRIPT <<EOF
kafka-console-consumer.sh \\
  --bootstrap-server ${HOSTNAME}:${BROKER_PORT} \\
  --topic \$TOPIC \\
  --from-beginning
EOF
    fi

    chmod +x $CONSUMER_SCRIPT
    echo "Consumer script generated at $CONSUMER_SCRIPT"
}

# ==============================
# Kafka Health Check
# ==============================
kafka_health_check() {
    echo "Checking Kafka status..."
    sleep 5
    if [ "$ENABLE_SSL" = true ]; then
        kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_PORT} --list --command-config $KAFKA_BASE/config/ssl.properties
    else
        kafka-topics.sh --bootstrap-server ${HOSTNAME}:${BROKER_PORT} --list
    fi

    if [ $? -eq 0 ]; then
        echo "Kafka is running correctly."
    else
        echo "Kafka health check failed!"
    fi
}

# ==============================
# Create systemd service for Kafka
# ==============================
create_systemd_service() {
    SERVICE_FILE=/etc/systemd/system/kafka.service
    echo "Creating systemd service at $SERVICE_FILE"

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
# Start Kafka (systemd or direct)
# ==============================
start_kafka() {
    if [ "$USE_SYSTEMD" = true ]; then
        echo "Starting Kafka via systemd..."
        create_systemd_service
        sudo systemctl start kafka
        echo "Kafka started via systemd. Use: sudo systemctl status kafka"
    else
        echo "Starting Kafka directly (without systemd)..."
        kafka-server-start.sh -daemon $KAFKA_BASE/config/server.properties
        echo "Kafka started in background. Use 'ps aux | grep kafka' to check."
    fi
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
# Execute specified function
# ==============================
"$@"

: <<'COMMENT'
here is a how to use guide
./kafka_script.sh phase1
./kafka_script.sh phase2
./kafka_script.sh phase3
$KAFKA_BASE/run_producer.sh my_topic
$KAFKA_BASE/run_consumer.sh my_topic
./kafka_script.sh kafka_health_check
COMMENT
