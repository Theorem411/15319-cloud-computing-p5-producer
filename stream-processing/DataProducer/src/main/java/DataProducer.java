import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DataProducer {
    private Producer<String, String> producer;
    private String traceFileName;

    private Integer numPartitions = 5;

    public DataProducer(Producer producer, String traceFileName) {
        this.producer = producer;
        this.traceFileName = traceFileName;
    }

    /**
     * Task 1:
     * In Task 1, you need to read the content in the tracefile we give to you,
     * create two streams, and feed the messages in the tracefile to different
     * streams based on the value of "type" field in the JSON string.
     * 
     * Please note that you're working on an ec2 instance, but the streams should
     * be sent to your samza cluster. Make sure you can consume the topics on the
     * master node of your samza cluster before you make a submission.
     */
    public void sendData() {
        try (BufferedReader br = new BufferedReader(new FileReader(traceFileName))) {
            JsonParser parser = new JsonParser();
            String line = null;
            while ((line = br.readLine()) != null) {
                JsonObject jsonObj = parser.parse(line).getAsJsonObject();
                // Producer Record: topic, partition number, (opt) key, value
                /** Task 2 ONLY */
                // String type = jsonObj.get("type").getAsString();
                // String topic = (type.equals("DRIVER_LOCATION"))
                //         ? "driver-locations"
                //         : "events";

                // Integer blockId = jsonObj.get("blockId").getAsInt();
                // producer.send(new ProducerRecord<String, String>(
                //         topic, /* topic */
                //         blockId % numPartitions, /* partition id */
                //         null, /* key */
                //         line /* value */
                // ));
                
                /** Task 3 ONLY */
                String type = jsonObj.get("type").getAsString();
                if (type.equals("RIDER_INTEREST") || type.equals("RIDER_STATUS")) {
                    // if input's type is RIDER_INTEREST or RIDER_STATUS, send to all partitions
                    for (int pid = 0; pid < numPartitions; pid++) {
                        producer.send(new ProducerRecord<String, String>(
                                "events", /* topic */
                                pid, /* partition id */
                                null, /* key */
                                line /* value */
                        ));
                    }
                } else if (type.equals("RIDE_REQUEST")) {
                    Integer blockId = jsonObj.get("blockId").getAsInt();
                    producer.send(new ProducerRecord<String, String>(
                            "events", /* topic */
                            blockId % numPartitions, /* partition id */
                            null, /* key */
                            line /* value */
                    ));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
