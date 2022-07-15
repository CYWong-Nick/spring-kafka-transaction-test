package com.nick.kafka.listener;

import com.nick.kafka.exception.DeadLetterException;
import com.nick.kafka.exception.RetryException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

public class SimpleBatchListener implements BatchAcknowledgingMessageListener<String, String> {
    private static final Logger logger = LoggerFactory.getLogger(SimpleBatchListener.class);
    private final KafkaTemplate<String, String> kafkaTemplate;

    public SimpleBatchListener(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void onMessage(List<ConsumerRecord<String, String>> data, Acknowledgment acknowledgment) {
        for (int i = 0; i < data.size(); i++) {
            ConsumerRecord<String, String> record = data.get(i);
            long offset = record.offset();
            try {
                process(record);
            } catch (DeadLetterException e) {
                logger.info("Publishing message offset={} to dead letter", offset);
                this.kafkaTemplate.send("output-topic-dlt", record.partition(), record.key(), record.value());
                acknowledgment.nack(i + 1, 100);
                return;
            } catch (RetryException e) {
                logger.info("Retrying message offset={}", offset);
                acknowledgment.nack(i, 100);
                return;
            }
        }

        acknowledgment.acknowledge();
    }

    private void process(ConsumerRecord<String, String> record) throws DeadLetterException, RetryException {
        String content = record.value();
        switch (content) {
            case "NORMAL":
                this.kafkaTemplate.send("output-topic", "OUTPUT");
                break;
            case "RETRY":
                throw new RetryException();
            case "DEAD_LETTER":
                throw new DeadLetterException();
        }
    }
}
