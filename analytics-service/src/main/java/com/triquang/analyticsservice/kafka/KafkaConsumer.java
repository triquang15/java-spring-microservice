package com.triquang.analyticsservice.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import patient.events.PatientEvent;

@Service
public class KafkaConsumer {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(KafkaConsumer.class);

    @KafkaListener(topics = "patient", groupId = "analytics-service")
    public void consumerEvent(byte[] message) {
        try {
            PatientEvent patientEvent = PatientEvent.parseFrom(message);
            log.info("Received patient event: {}", patientEvent);
        } catch (InvalidProtocolBufferException e) {
            log.error("Failed to parse patient event", e);
            throw new RuntimeException(e);
        }
    }
}
