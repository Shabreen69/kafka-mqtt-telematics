package com.shabreen.telematics.mqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shabreen.telematics.model.VehicleTelemetry;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

/**
 * MqttTelematicsPublisher — simulates vehicle MQTT data publishing
 * for test scenarios. Publishes to topics that backend services consume.
 *
 * Topic structure: vehicles/{vehicleId}/telemetry
 */
public class MqttTelematicsPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MqttTelematicsPublisher.class);
    private final MqttClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    private static final int QOS_AT_LEAST_ONCE = 1;
    private static final String TOPIC_TEMPLATE  = "vehicles/%s/telemetry";
    private static final String ALERT_TEMPLATE  = "vehicles/%s/alerts";

    public MqttTelematicsPublisher(String brokerUrl) throws MqttException {
        String clientId = "test-publisher-" + UUID.randomUUID().toString().substring(0, 8);
        this.client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());

        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setCleanSession(true);
        opts.setConnectionTimeout(30);
        opts.setKeepAliveInterval(60);
        opts.setAutomaticReconnect(true);

        client.connect(opts);
        log.info("MQTT publisher connected to {}", brokerUrl);
    }

    public void publishTelemetry(VehicleTelemetry telemetry) throws Exception {
        String topic = String.format(TOPIC_TEMPLATE, telemetry.getVehicleId());
        String payload = mapper.writeValueAsString(telemetry);
        MqttMessage message = new MqttMessage(payload.getBytes());
        message.setQos(QOS_AT_LEAST_ONCE);
        message.setRetained(false);
        client.publish(topic, message);
        log.info("Published telemetry for vehicle={} soc={}%",
                telemetry.getVehicleId(), telemetry.getStateOfCharge());
    }

    public void publishBurst(String vehicleId, int count, long intervalMs) throws Exception {
        for (int i = 0; i < count; i++) {
            VehicleTelemetry telemetry = VehicleTelemetry.builder()
                    .vehicleId(vehicleId)
                    .stateOfCharge(80.0f - (i * 0.5f))
                    .speedKmh(45.0f + (i % 10))
                    .temperatureCelsius(30.0f + (i * 0.1f))
                    .latitudeDeg(12.9716f + (i * 0.0001f))
                    .longitudeDeg(77.5946f + (i * 0.0001f))
                    .timestampEpochMs(System.currentTimeMillis())
                    .build();
            publishTelemetry(telemetry);
            if (intervalMs > 0) Thread.sleep(intervalMs);
        }
        log.info("Published burst of {} events for vehicle={}", count, vehicleId);
    }

    public void simulateDeepDischarge(String vehicleId) throws Exception {
        VehicleTelemetry critical = VehicleTelemetry.builder()
                .vehicleId(vehicleId)
                .stateOfCharge(2.5f)        // Below 5% threshold
                .voltageV(43.8f)
                .temperatureCelsius(31.0f)
                .timestampEpochMs(System.currentTimeMillis())
                .build();
        publishTelemetry(critical);
        log.warn("Simulated DEEP DISCHARGE for vehicle={}", vehicleId);
    }

    public void simulateOverheating(String vehicleId) throws Exception {
        VehicleTelemetry overheat = VehicleTelemetry.builder()
                .vehicleId(vehicleId)
                .stateOfCharge(65.0f)
                .temperatureCelsius(62.0f)   // Above 55°C threshold
                .voltageV(57.0f)
                .timestampEpochMs(System.currentTimeMillis())
                .build();
        publishTelemetry(overheat);
        log.warn("Simulated OVERHEATING for vehicle={}", vehicleId);
    }

    @Override
    public void close() throws MqttException {
        if (client.isConnected()) {
            client.disconnect();
        }
        client.close();
    }
}
