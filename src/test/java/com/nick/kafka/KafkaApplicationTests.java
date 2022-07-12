package com.nick.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:3333",
                "port=3333",
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1"
        }
)
@DirtiesContext
class KafkaApplicationTests {

    @Autowired
    private EmbeddedKafkaBroker broker;

    private KafkaTemplate<String, String> kafkaTemplate;
    private AdminClient adminClient;
    private KafkaConsumer<String, String> consumer;
    private KafkaConsumer<String, String> deadLetterConsumer;

    @BeforeEach
    public void beforeEach() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps, new StringSerializer(), new StringSerializer());
        this.consumer = configureConsumer("test-group", "output-topic");
        this.deadLetterConsumer = configureConsumer("test-dlt-group", "output-topic-dlt");
        this.kafkaTemplate = new KafkaTemplate<>(producerFactory);
        this.adminClient = KafkaAdminClient.create(producerProps);
    }

    private KafkaConsumer<String, String> configureConsumer(String group, String topic) {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(group, "true", broker);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(Collections.singleton(topic));

        return consumer;
    }

    @Test
    void testNormal() throws InterruptedException, ExecutionException {
        sendNormal();
        sendNormal();
        sendNormal();
        sendNormal();
        sendNormal();

        awaitAndValidateOffset(5);
        pollAndValidatePublishedCount(5);
    }

    @Test
    void testRetry() throws InterruptedException, ExecutionException {
        sendNormal();
        sendNormal();
        sendNormal();
        sendRetry();
        sendNormal();

        awaitAndValidateOffset(3);
        pollAndValidatePublishedCount(3);
    }

    @Test
    void testDeadLetter() throws InterruptedException, ExecutionException {
        sendNormal();
        sendNormal();
        sendNormal();
        sendDeadLetter();
        sendNormal();

        awaitAndValidateOffset(5);
        pollAndValidatePublishedCount(4);
        pollAndValidateDeadLetterCount(1);
    }

    private void awaitAndValidateOffset(int expected) throws InterruptedException, ExecutionException {
        Thread.sleep(5000);
        long offset = getInputOffset();
        assertThat(offset).isEqualTo(expected);
    }

    private List<ConsumerRecord<String, String>> pollAndValidateCount(KafkaConsumer<String, String> consumer, int expected) {
        List<ConsumerRecord<String, String>> records = StreamSupport.stream(
                        consumer.poll(Duration.ofSeconds(3)).spliterator(),
                        false
                )
                .collect(Collectors.toList());

        assertThat(records).hasSize(expected);

        return records;
    }

    private List<ConsumerRecord<String, String>> pollAndValidateDeadLetterCount(int expected) {
        return pollAndValidateCount(this.deadLetterConsumer, expected);
    }

    private List<ConsumerRecord<String, String>> pollAndValidatePublishedCount(int expected) {
        return pollAndValidateCount(this.consumer, expected);
    }

    private long getInputOffset() throws InterruptedException, ExecutionException {
        Map<TopicPartition, OffsetAndMetadata> offsets = adminClient.listConsumerGroupOffsets("group-id").partitionsToOffsetAndMetadata().get();
        Iterator<OffsetAndMetadata> iterator = offsets.values().iterator();
        return iterator.hasNext() ? iterator.next().offset() : 0L;
    }

    private void sendNormal() {
        kafkaTemplate.send("input-topic", "NORMAL");
    }

    private void sendRetry() {
        kafkaTemplate.send("input-topic", "RETRY");
    }

    private void sendDeadLetter() {
        kafkaTemplate.send("input-topic", "DEAD_LETTER");
    }
}
