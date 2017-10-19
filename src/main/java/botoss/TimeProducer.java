package botoss;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

public class TimeProducer {
    public static final String DATE_FORMAT = "HH:mm:ss (zZ)\ndd MMMM y (EEEE)";
    public static final String DEFAULT_TIME_ZONE = "Europe/Moscow";
    static void time(ConsumerRecord<String, String> record) throws IOException {
        JSONObject jobj = new JSONObject(record.value());
        Properties props = new Properties();
        try (Reader propsReader = new FileReader("/kafka.properties")) {
            props.load(propsReader);
        }
        JSONArray params = jobj.getJSONArray("params");
        String timeZone = DEFAULT_TIME_ZONE;
        if (params.length() > 0) {
            timeZone = params.getString(0);
        }
        Producer<String, String> producer = new KafkaProducer<>(props);
        JSONObject ans = new JSONObject();
        try {
            ans.put("connector-id", jobj.getString("connector-id"));
        } catch (JSONException ignore) {
            // it's ok for now not to have connector-id in message
        }
        DateFormat date = new SimpleDateFormat(DATE_FORMAT);
        date.setTimeZone(TimeZone.getTimeZone(timeZone));
        ans.put("text", date.format(new Date()));

        producer.send(new ProducerRecord<>("to-connector", record.key(), ans.toString()));

        producer.close();
    }
}
