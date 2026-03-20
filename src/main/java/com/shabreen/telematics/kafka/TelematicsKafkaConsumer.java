package com.shabreen.telematics.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.shabreen.telematics.model.VehicleTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;

/**
 * TelematicsKafkaConsumer — test utility for consuming and asserting
 * on real-time vehicle telematics events from Kafka topics.
 *
 * Used to verify backend processes vehicle data correctly
 * (ingestion, anomaly detection, alert generation).
 */
public class TelematicsKafkaConsumer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TelematicsKafkaConsumer.class);
    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final List<VehicleTelemetry> collectedEvents = new CopyOnWriteArrayList<>();

    public TelematicsKafkaConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId + "-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        this.consumer = new KafkaConsumer<>(props);
    }

    public void subscribe(String... topics) {
        consumer.subscribe(Arrays.asList(topics));
        log.info("Subscribed to topics: {}", Arrays.toString(topics));
    }

    /**
     * Poll Kafka until the predicate matches at least one event
     * or the timeout expires.
     */
    public Optional<VehicleTelemetry> waitForEvent(
            Predicate<VehicleTelemetry> condition, Duration timeout) {

        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    VehicleTelemetry event = mapper.readValue(record.value(), VehicleTelemetry.class);
                    collectedEvents.add(event);
                    if (condition.test(event)) {
                        log.info("Matched event from partition={} offset={}", record.partition(), record.offset());
                        return Optional.of(event);
                    }
                } catch (Exception e) {
                    log.warn("Failed to deserialise record: {}", record.value(), e);
                }
            }
        }
        return Optional.empty();
    }

    /**
     * Collect all events for a specific vehicle within a time window.
     */
    public List<VehicleTelemetry> collectEventsForVehicle(String vehicleId, Duration window) {
        List<VehicleTelemetry> vehicleEvents = new ArrayList<>();
        long deadline = System.currentTimeMillis() + window.toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    VehicleTelemetry event = mapper.readValue(record.value(), VehicleTelemetry.class);
                    if (vehicleId.equals(event.getVehicleId())) {
                        vehicleEvents.add(event);
                    }
                } catch (Exception e) {
                    log.warn("Parse error: {}", e.getMessage());
                }
            }
        }
        return vehicleEvents;
    }

    public List<VehicleTelemetry> getCollectedEvents() {
        return Collections.unmodifiableList(collectedEvents);
    }

    public void clearCollected() {
        collectedEvents.clear();
    }

    @Override
    public void close() {
        consumer.close();
    }
}
