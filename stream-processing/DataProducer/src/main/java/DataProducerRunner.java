import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

public class DataProducerRunner {
    private static String kafkaMasterNodeIp = "172.31.17.109:9092"; // TODO: fill in

    public static void main(String[] args) throws Exception {
        /*
         * Tasks to complete:
         * - Write enough tests in the DataProducerTest.java file
         * - Instantiate the Kafka Producer by following the API documentation
         * - Instantiate the DataProducer using the appropriate trace file and the
         * producer
         * - Implement the sendData method as required in DataProducer
         * - Call the sendData method to start sending data
         */
        // set up Kafka Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaMasterNodeIp);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // call DataProducer to send data stream to Kafka
        DataProducer dataProducer = new DataProducer(producer, "trace_bonus");
        dataProducer.sendData();

        /// DEBUG: close producer
        producer.close();
    }
}
