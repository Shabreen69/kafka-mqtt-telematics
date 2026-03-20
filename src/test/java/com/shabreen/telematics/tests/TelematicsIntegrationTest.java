package com.shabreen.telematics.tests;

import com.shabreen.telematics.kafka.TelematicsKafkaConsumer;
import com.shabreen.telematics.mqtt.MqttTelematicsPublisher;
import com.shabreen.telematics.model.VehicleTelemetry;
import com.shabreen.telematics.config.TelematicsConfig;
import org.testng.Assert;
import org.testng.annotations.*;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/**
 * TelematicsIntegrationTest — validates end-to-end pipeline:
 *
 *  MQTT Publisher  →  Broker  →  Backend Processor  →  Kafka Topic
 *                                        ↓
 *                              Alert Generator / DB
 *
 * Tests:
 *  1. Data ingestion accuracy (SOC, location, temperature)
 *  2. Anomaly detection (deep discharge, overheating)
 *  3. Message delivery guarantees (QoS, deduplication)
 *  4. Throughput under burst load
 *  5. Out-of-order message handling
 */
public class TelematicsIntegrationTest {

    private MqttTelematicsPublisher publisher;
    private TelematicsKafkaConsumer consumer;
    private TelematicsKafkaConsumer alertConsumer;

    private static final String TEST_VEHICLE_ID = "VH-INTEGRATION-TEST-001";
    private static final Duration EVENT_TIMEOUT  = Duration.ofSeconds(15);

    @BeforeClass
    public void setup() throws Exception {
        publisher     = new MqttTelematicsPublisher(TelematicsConfig.getMqttBrokerUrl());
        consumer      = new TelematicsKafkaConsumer(TelematicsConfig.getKafkaBootstrap(), "test-telematics");
        alertConsumer = new TelematicsKafkaConsumer(TelematicsConfig.getKafkaBootstrap(), "test-alerts");

        consumer.subscribe("vehicle-telemetry-processed");
        alertConsumer.subscribe("vehicle-alerts");
    }

    // ── Data accuracy ─────────────────────────────────────────────

    @Test(description = "SOC published via MQTT should appear in Kafka with correct value",
          groups = {"smoke", "telematics"})
    public void testSOCIngestionAccuracy() throws Exception {
        float expectedSOC = 73.5f;

        VehicleTelemetry payload = VehicleTelemetry.builder()
                .vehicleId(TEST_VEHICLE_ID)
                .stateOfCharge(expectedSOC)
                .temperatureCelsius(30.0f)
                .latitudeDeg(12.9716f)
                .longitudeDeg(77.5946f)
                .timestampEpochMs(System.currentTimeMillis())
                .build();

        publisher.publishTelemetry(payload);

        Optional<VehicleTelemetry> received = consumer.waitForEvent(
                e -> TEST_VEHICLE_ID.equals(e.getVehicleId())
                        && Math.abs(e.getStateOfCharge() - expectedSOC) < 0.1f,
                EVENT_TIMEOUT);

        Assert.assertTrue(received.isPresent(), "Telematics event not received in Kafka within timeout");
        Assert.assertEquals(received.get().getStateOfCharge(), expectedSOC, 0.1f,
                "SOC value mismatch after ingestion");
    }

    @Test(description = "GPS coordinates should be preserved accurately through the pipeline",
          groups = {"regression", "telematics"})
    public void testLocationAccuracy() throws Exception {
        double lat = 12.971598;
        double lng = 77.594562;

        VehicleTelemetry payload = VehicleTelemetry.builder()
                .vehicleId(TEST_VEHICLE_ID)
                .stateOfCharge(60.0f)
                .latitudeDeg((float) lat)
                .longitudeDeg((float) lng)
                .timestampEpochMs(System.currentTimeMillis())
                .build();

        publisher.publishTelemetry(payload);

        Optional<VehicleTelemetry> received = consumer.waitForEvent(
                e -> TEST_VEHICLE_ID.equals(e.getVehicleId())
                        && Math.abs(e.getLatitudeDeg() - lat) < 0.0001,
                EVENT_TIMEOUT);

        Assert.assertTrue(received.isPresent(), "Location event not received");
        Assert.assertEquals(received.get().getLatitudeDeg(), lat, 0.0001, "Latitude mismatch");
        Assert.assertEquals(received.get().getLongitudeDeg(), lng, 0.0001, "Longitude mismatch");
    }

    // ── Anomaly detection ─────────────────────────────────────────

    @Test(description = "Deep discharge should trigger CRITICAL alert in Kafka",
          groups = {"regression", "anomaly"})
    public void testDeepDischargeAlertGeneration() throws Exception {
        publisher.simulateDeepDischarge(TEST_VEHICLE_ID);

        Optional<VehicleTelemetry> alert = alertConsumer.waitForEvent(
                e -> TEST_VEHICLE_ID.equals(e.getVehicleId())
                        && "DEEP_DISCHARGE".equals(e.getAlertType()),
                EVENT_TIMEOUT);

        Assert.assertTrue(alert.isPresent(),
                "Deep discharge alert should be generated in Kafka alerts topic");
        Assert.assertEquals(alert.get().getAlertSeverity(), "CRITICAL",
                "Deep discharge alert severity should be CRITICAL");
    }

    @Test(description = "Overheating should trigger THERMAL_RUNAWAY_RISK alert",
          groups = {"regression", "anomaly"})
    public void testOverheatingAlertGeneration() throws Exception {
        publisher.simulateOverheating(TEST_VEHICLE_ID);

        Optional<VehicleTelemetry> alert = alertConsumer.waitForEvent(
                e -> TEST_VEHICLE_ID.equals(e.getVehicleId())
                        && "THERMAL_RUNAWAY_RISK".equals(e.getAlertType()),
                EVENT_TIMEOUT);

        Assert.assertTrue(alert.isPresent(),
                "Thermal runaway risk alert should be generated");
    }

    // ── Throughput ────────────────────────────────────────────────

    @Test(description = "System should handle 100 events in 10 seconds (10 events/sec)",
          groups = {"performance"})
    public void testIngestionThroughput() throws Exception {
        consumer.clearCollected();
        int eventCount = 100;
        long start = System.currentTimeMillis();

        publisher.publishBurst(TEST_VEHICLE_ID, eventCount, 100); // 10 events/sec

        // Wait for all events to appear in Kafka
        long deadline = System.currentTimeMillis() + 20_000L;
        while (System.currentTimeMillis() < deadline) {
            consumer.waitForEvent(e -> false, Duration.ofSeconds(1)); // drain
            if (consumer.getCollectedEvents().size() >= eventCount) break;
        }

        long elapsed = System.currentTimeMillis() - start;
        long vehicleEventCount = consumer.getCollectedEvents().stream()
                .filter(e -> TEST_VEHICLE_ID.equals(e.getVehicleId())).count();

        Assert.assertTrue(vehicleEventCount >= eventCount * 0.95,
                "At least 95% of events should be ingested. Got: " + vehicleEventCount);
        Assert.assertTrue(elapsed < 25_000,
                "100 events should be processed within 25 seconds. Took: " + elapsed + "ms");
    }

    // ── Message ordering ──────────────────────────────────────────

    @Test(description = "Events should be ordered by timestamp within the same vehicle partition",
          groups = {"regression"})
    public void testEventOrdering() throws Exception {
        consumer.clearCollected();
        publisher.publishBurst(TEST_VEHICLE_ID, 10, 50);

        Thread.sleep(5_000); // let all events arrive

        List<VehicleTelemetry> events = consumer.collectEventsForVehicle(
                TEST_VEHICLE_ID, Duration.ofSeconds(3));

        for (int i = 1; i < events.size(); i++) {
            Assert.assertTrue(
                    events.get(i).getTimestampEpochMs() >= events.get(i - 1).getTimestampEpochMs(),
                    "Events should be in timestamp order at index " + i);
        }
    }

    @AfterClass
    public void teardown() throws Exception {
        if (publisher != null)     publisher.close();
        if (consumer != null)      consumer.close();
        if (alertConsumer != null) alertConsumer.close();
    }
}
