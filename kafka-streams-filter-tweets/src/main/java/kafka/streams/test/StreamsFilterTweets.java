package kafka.streams.test;


import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsFilterTweets {
    Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());
    public static void main(String[] args) {

        //creating properties
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "35.239.201.205:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create streams builder
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputTopics = streamsBuilder.stream("twitter_input");
        KStream<String, String> streamFiltered = inputTopics.filter(
                (k, v) -> filteredTweet(v) > 10
        );
        streamFiltered.to("filtered_tweet");

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),props);

        kafkaStreams.start();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static Integer filteredTweet(String jsonText){

        return jsonParser.parse(jsonText)
                .getAsJsonObject()
                .get("user")
                .getAsJsonObject()
                .get("followers_count")
                .getAsInt();

    }
}
