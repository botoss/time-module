package botoss;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

public class TimeProducer {
    static void time(ConsumerRecord<String, String> record) throws IOException {
        JSONObject jobj = new JSONObject(record.value());
        Properties props = new Properties();
        try (Reader propsReader = new FileReader("/kafka.properties")) {
            props.load(propsReader);
        }
        Producer<String, String> producer = new KafkaProducer<>(props);
        JSONObject ans = new JSONObject();
        try {
            ans.put("connector-id", jobj.getString("connector-id"));
        } catch (JSONException ignore) {
            // it's ok for now not to have connector-id in message
        }
         ans.put("text", new SimpleDateFormat("HH:mm:ss\ndd MMMM y (EEEE)").format(Calendar.getInstance(TimeZone.getTimeZone("Europe/Moscow")).getTime()));

        producer.send(new ProducerRecord<>("to-connector", record.key(), ans.toString()));

        producer.close();
    }
}
