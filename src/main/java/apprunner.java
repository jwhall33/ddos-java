import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.api.java.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;

import static org.apache.directory.api.util.Strings.isEmpty;

public class apprunner {
            private static final String HOST = "localhost";
            private static final int PORT = 9092;
            private static final String CHECKPOINT_DIR = "/tmp";
            private static final Duration BATCH_DURATION = Durations.seconds(15);
            private static final Logger logger = LoggerFactory.getLogger(apprunner.class);

            private static final String IPADDRESS_PATTERN =
                    "(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\\\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)";


            public static void main(String[] args) throws InterruptedException {
                logger.info("Starting App");
                // Configure and initialize the SparkStreamingContext
                SparkConf conf = new SparkConf()
                        .setMaster("local[*]")
                        .setAppName("DetectDDOS");
                logger.info("Setting streaming context");
                JavaStreamingContext streamingContext =
                        new JavaStreamingContext(conf, BATCH_DURATION);
                streamingContext.sparkContext().setLogLevel("WARN");

                //Logger.getRootLogger().setLevel(Level.ERROR);
                streamingContext.checkpoint(CHECKPOINT_DIR);

                List<String> topics = Collections.singletonList("http-log");
                Map<String, Object> kafkaParams = new HashMap<>();
                kafkaParams.put("bootstrap.servers", "localhost:9092");
                //kafkaParams.put("auto.offset.reset", "earliest");
                kafkaParams.put("key.deserializer", StringDeserializer.class);
                kafkaParams.put("value.deserializer", StringDeserializer.class);
                kafkaParams.put("group.id", "main");

                logger.info("Ready to stream");
                JavaInputDStream<ConsumerRecord<String, String>> directKafkaStream = KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

                JavaPairDStream<String, Integer> messages = directKafkaStream.mapToPair(record -> {
                    String ip = StringUtils.substringBefore(record.value(), " ").replace("\"", "");
                    return new Tuple2<>(ip, 1);
                });

                Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                        new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
                            @Override
                            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) {
                                Integer newSum = state.orElse(0);
                                for(int i : values)
                                {
                                    newSum += i;
                                }
                                return Optional.of(newSum);
                            }
                        };

                JavaPairDStream<String, Integer> parsed = messages.updateStateByKey(updateFunction);

                JavaPairDStream<String, Integer> httpThreat = parsed.filter( pair -> pair._2 > 88);

                httpThreat.print();


                logger.info("starting context");
                streamingContext.start();
                logger.info("awaiting termination");
                streamingContext.awaitTermination();
            }
        }
