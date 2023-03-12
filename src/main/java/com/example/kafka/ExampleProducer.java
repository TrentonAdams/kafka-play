package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class ExampleProducer<T> implements AutoCloseable
{
    String topic;
    public ExampleProducer(String topic){

        this.topic = topic;
    }

    private KafkaProducer<String, T> producer;

    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    void send(T message){
        ProducerRecord<String, T> record = new ProducerRecord<>(topic, null,
            message);
        producer.send(record);
    }

    @Override
    public void close() throws Exception
    {
        producer.close();
    }
}