package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.function.Consumer;

public class ExampleConsumer<T> implements AutoCloseable
{

    private final KafkaConsumer<String, T> consumer;
    private String topic;
    private KafkaExampleThread<T> kafkaThread;

    public ExampleConsumer(String topic)
    {
        this.topic = topic;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "my-consumer-instance");

        consumer = new KafkaConsumer<>(props);
    }

    public void startConsumer(java.util.function.Consumer<T> consumerResult)
    {

        kafkaThread = new KafkaExampleThread<>(consumer, topic, consumerResult);
        kafkaThread.start();
    }

    @Override
    public void close() throws Exception
    {
        kafkaThread.close();
    }

    static class KafkaExampleThread<T> extends Thread implements AutoCloseable
    {
        private KafkaConsumer<String, T> consumer;

        public KafkaExampleThread(final KafkaConsumer<String, T> consumer,
            final String topic, final Consumer<T> consumerResult)
        {
            super(() -> {
                consumer.subscribe(Collections.singletonList(topic));
                try
                {
                    while (true)
                    {
                        ConsumerRecords<String, T> records = consumer.poll(
                            Duration.ofMillis(100));
                        records.forEach(record -> {
                            consumerResult.accept(record.value());
                            System.out.printf(
                                "Received message: key = %s, value = %s, partition = %d, offset = %d%n",
                                record.key(), record.value(), record.partition(),
                                record.offset());
                        });
                        consumer.commitSync();
                    }
                }
                catch(WakeupException e)
                {
                    // ignored, just waking up
                }
                finally
                {
                    consumer.unsubscribe();
                    consumer.close();
                }
            });

            this.consumer = consumer;
        }

        @Override
        public void close() throws Exception
        {
            new Thread(() -> {
                consumer.wakeup();
            }).start();
        }
    }
}
