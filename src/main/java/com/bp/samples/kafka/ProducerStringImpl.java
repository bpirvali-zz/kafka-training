package com.bp.samples.kafka;

import com.sun.istack.NotNull;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by behzad.pirvali on 1/8/17.
 */
public class ProducerStringImpl implements ProducerString {
    private static final Logger logger = LoggerFactory.getLogger(ProducerStringImpl.class);

    private String zkHostPort;
    private String brokerList;
    private KafkaProducer<String, String> producer = null;

    public ProducerStringImpl(String zkHostPort, String brokerList) {
        this.zkHostPort = zkHostPort;
        this.brokerList = brokerList;
        this.producer = creProducerForString();
    }

    @Override
    public void send(@NotNull String topic, @NotNull String key, @NotNull String value) {
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
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

}
