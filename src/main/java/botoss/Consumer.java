package botoss;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;

public class Consumer {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        try (Reader propsReader = new FileReader("/kafka.properties")) {
            props.load(propsReader);
        }
        props.put("group.id", "time-module");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("to-module"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                String command = (new JSONObject(record.value())).getString("command");
                if (rateCommand(command)) {
                    TimeProducer.time(record);
                }
            }
        }
    }

    private static boolean rateCommand(String command) {
        return Arrays.asList("time", "время", "date", "дата").contains(command);
    }
}
