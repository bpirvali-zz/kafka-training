package com.bp.samples.util;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by behzad.pirvali on 1/7/17.
 */
//@Component
public class KafkaHelperImpl implements KafkaHelper {
    private static final Logger logger = LoggerFactory.getLogger(KafkaHelperImpl.class);

    private String zkHostPort;
    private String brokerList;
    private KafkaProducer<String, String> producer = null;

    public KafkaHelperImpl(String zkHostPort, String brokerList) {
        this.zkHostPort = zkHostPort;
        this.brokerList = brokerList;
        this.producer = creProducerForString();
    }

    @Override
    public void sendString(String topic, String key, String value) {
        try {
            if (producer==null)
                throw new RuntimeException("ProducerString is closed probably due to an exception, please see the logs!");

            ProducerRecord record = new ProducerRecord(topic, key, value );
            producer.send(record);
//            logger.info()
        } catch (Exception e) {
            logger.error("Exception in sendString, closing producer...", e);
            producer.close();
            producer = null;
        }
    }

    @Override
    public void close() {
        producer.close();
    }

    private KafkaProducer<String, String> creProducerForString() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> creConsumerForString() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("camel-demo"));
        //consumer.unsubscribe();
        consumer =  new KafkaConsumer<String, String>(props);

        List<String> topics = new ArrayList<String>();
        topics.add("camel-demo");
        topics.add("camel-demo-2");
        consumer.subscribe(topics);

        return consumer;
    }
}
