package com.bp.samples;

import com.bp.samples.kafka.ProducerString;
import com.bp.samples.kafka.ProducerStringImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.SimpleCommandLinePropertySource;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@SpringBootApplication
public class KafkaSampleApp implements CommandLineRunner {
	private static final Logger logger = LoggerFactory.getLogger(KafkaSampleApp.class);
	@Value("${kafka.topic}")
	private String topic;

	@Value("${kafka.topic2}")
	private String topic2;

	@Value("${kafka.bootstrap.servers}")
	private String brokerList;

	@Value("${kafka.zk.host}")
	private String zkHost;

	@Value("${kafka.zk.port}")
	private int zkPort;

    private SimpleCommandLinePropertySource clArgs;

	public static void main(String[] args) {
        SpringApplication.run(KafkaSampleApp.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
        clArgs = new SimpleCommandLinePropertySource(args);
        if (clArgs==null || clArgs.getPropertyNames().length==0) {
            printUsage();
        } else {
            if (runAs("producer")) {
                runProducer();
            } else if (runAs("consumer")) {
                runConsumer();
            } else if (runAs("assignedConsumer")) {
                runAssignedConsumer();
            } else {
                printUsage();
            }
        }
	}

	private int getNumMsgs(SimpleCommandLinePropertySource clArgs) {
        int numMsgs = 1;
        String s = clArgs.getProperty("numMsgs");

        if (s!=null)
            numMsgs = Integer.parseInt(s);
        return numMsgs;
    }

    private void runProducer() {
        ProducerString producer = new ProducerStringImpl(zkHost + ":" + zkPort, brokerList);
        int numMsgs = getNumMsgs(clArgs);
        for (int i=0; i<numMsgs; i++) {
            String msg = "My Message " + i;
            logger.info("Sending msg:{}", msg);
            producer.send(topic, Integer.toString(i), msg );

        }
        producer.close();
        logger.info("producer closed!");
    }

    private void runConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "test");

        KafkaConsumer<String,String> consumer =  new KafkaConsumer<String, String>(props);

        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        topics.add(topic2);
        consumer.subscribe(topics);
        logger.info("Starting Consumer poll...");
        try {
            while (true){
                ConsumerRecords records = consumer.poll(10);
                printRecords(records);
            }
        } catch (Exception e){
            logger.error("Consumer Exception!", e);
        }finally {
            consumer.close();
        }
        logger.info("Consumer closed!");
    }

    private void runAssignedConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "");
        KafkaConsumer<String,String> consumer =  new KafkaConsumer<String, String>(props);
        List<TopicPartition> partitions = new ArrayList<>();
        TopicPartition p0 = new TopicPartition(topic, 0);
        TopicPartition p1 = new TopicPartition(topic2, 0);
        TopicPartition p2 = new TopicPartition(topic2, 2);
        partitions.add(p0);
        partitions.add(p1);
        partitions.add(p2);
        consumer.assign(partitions);

//        List<String> topics = new ArrayList<String>();
//        topics.add("camel-demo");
//        topics.add("camel-demo-2");
//        consumer.subscribe(topics);

        logger.info("Starting Consumer poll...");
        try {
            while (true){
                ConsumerRecords records = consumer.poll(10);
                printRecords(records);
            }
        } catch (Exception e){
            logger.error("Consumer Exception!", e);
        }finally {
            consumer.close();
        }
        logger.info("Consumer closed!");
    }

    private static void printRecords(ConsumerRecords<String, String> records)
    {
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
        }
    }


    private boolean runAs(String kafkaClient) {
        if (clArgs.getProperty("type").equalsIgnoreCase(kafkaClient))
            return true;
        return false;
    }

    private void printUsage() {
        System.out.println("--------------------------------------------------------------------------------------------------------");
        System.out.println("Usage: gradle bootrun -Pargs='--type=<producer|consumer|assignedConsumer> [--numMsgs=<#, default=1>]'");
        System.out.println("--------------------------------------------------------------------------------------------------------");
    }
}
