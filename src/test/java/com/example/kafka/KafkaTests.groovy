package com.example.kafka

import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.atomic.AtomicReference

class KafkaTests extends Specification
{
    def "send and consume"()
    {
        String topic = "quickstart-events"
        given:
            PollingConditions conditions = new PollingConditions(timeout: 5,
                    initialDelay: 1.5, factor: 1.25)
            ExampleProducer producer = new ExampleProducer(topic)
            ExampleConsumer<String> consumer = new ExampleConsumer<>(
                    topic)
            // todo add two atomic lists (dequeue thing?) and compare what's sent vs received, and in what order.
            AtomicReference<String> atomicReference = new AtomicReference<>(
                    "nothing")
            consumer.startConsumer {
                System.out.println("Received consumer result: " + it)
                atomicReference.set(it)
            }

        when:
            String message = "test message ${UUID.randomUUID()}"
            producer.send(message)

        then:
            conditions.eventually {
                atomicReference.get() == message
            }

        when:
            message = "test message ${UUID.randomUUID()}"
            producer.send(message)

        then:
            conditions.eventually {
                atomicReference.get() == message
            }

        cleanup:
            producer.close()
            consumer.close()
    }
}
