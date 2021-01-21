import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Properties properties = new Properties();
                properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
                properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
                String message;
                JSONObject json = new JSONObject();
                json.put("id", UUID.randomUUID());
                json.put("firstName", "FirstName");
                json.put("lastName", "LastName");
                JSONArray array = new JSONArray();
                JSONObject item = new JSONObject();
                item.put("name", "CompanyName");
                item.put("role", "Title");
                array.put(item);
                json.put("Job", array);
                message = json.toString();
                // message
                // {
                // "firstName":"Hüseyin",
                // "lastName":"Kadıoğlu",
                // "id":"ba583765-a3d2-4088-9a51-4fe38ce1c5d9",
                // "Job":[
                // {"role":"Software Engineer",
                // "name":"Linktera Bilgi Teknolojileri"}
                // ]}
                ProducerRecord<String, String> record = new ProducerRecord<>("tube", null, message);
                producer.send(record);
                producer.flush();
            }
        }, 1, 1, TimeUnit.SECONDS);
    }
}
