package botoss;

import org.apache.commons.io.IOUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Date;
import java.util.Properties;

public class TimeProducer {
    private static final String TIME_URL = "https://yandex.com/time/sync.json?geo=0";
    private static String url = "";

    static {
        try {
            url = getUrl();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

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
        //ans.put("text", new Date((new JSONObject(url)).getLong("time") + (1000 * 60 * 60 * 3)));
        ans.put("text", new Date());

        producer.send(new ProducerRecord<>("to-connector", record.key(), ans.toString()));

        producer.close();
    }

    private static String getUrl() throws IOException {
        HttpGet req = new HttpGet(TIME_URL);
        req.setHeader("Content-Type", "application/json");
        try (CloseableHttpClient client = HttpClients.createDefault();
             CloseableHttpResponse response = client.execute(req)) {
            InputStream inputStream = response.getEntity().getContent();
            return IOUtils.toString(inputStream);
        }
    }


}
