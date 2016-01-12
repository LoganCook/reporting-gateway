package au.edu.ersa.reporting.kzk;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;

import au.edu.ersa.reporting.http.Util;
import au.edu.ersa.reporting.http.Wrap;
import au.edu.ersa.reporting.http.api.Broker;
import au.edu.ersa.reporting.http.api.Message;
import au.edu.ersa.reporting.http.api.Topic;
import au.edu.ersa.reporting.http.api.Topic.State;
import au.edu.ersa.reporting.http.api.TopicMessageMap;
import io.dropwizard.lifecycle.Managed;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

public class Kafka implements Managed {
    private final static Logger LOG = LoggerFactory.getLogger(Kafka.class);

    private static final Random RANDOM = new Random();

    private static final long EARLIEST = kafka.api.OffsetRequest.EarliestTime();
    private static final long LATEST = kafka.api.OffsetRequest.LatestTime();
    private static final short VERSION = kafka.api.OffsetRequest.CurrentVersion();

    private static final ObjectMapper JSON = new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

    private static final int MAX_MESSAGE_SIZE = 10 * 1024 * 1024;

    private final String me = toString().replace('@', '-');

    private final ZK zk;
    private KafkaProducer<String,String> producer;

    private KafkaProducer<String,String> newProducer() {
        Properties props = new Properties();

        props.put("bootstrap.servers", Joiner.on(',').join(listBrokers().stream().map(i -> {
            Broker broker = getBroker(Integer.parseInt(i));
            return broker.host + ":" + broker.port;
        }).collect(Collectors.toList())));
        props.put("max.request.size", Integer.toString(MAX_MESSAGE_SIZE));
        props.put("acks", "-1");

        return new KafkaProducer<String,String>(props, new StringSerializer(), new StringSerializer());
    };

    public Kafka(ZK zk) {
        this.zk = zk;
    }

    public List<String> listTopics() {
        return zk.list("/brokers/topics");
    }

    public Topic getTopic(String name) {
        if (!zk.exists("/brokers/topics/" + name)) {
            return null;
        }

        Topic topic = new Topic(name);


        String dataJSON = zk.get("/brokers/topics/" + name);
        if (dataJSON != null && !dataJSON.isEmpty()) {
            topic.summary = Wrap.runtimeException(() -> JSON.readValue(dataJSON, Topic.Data.class));
        }

        for (String partition : zk.list("/brokers/topics/" + name + "/partitions")) {
            String stateJSON = zk.get("/brokers/topics/" + name + "/partitions/" + partition + "/state");

            if (stateJSON != null && !stateJSON.isEmpty()) {
                topic.partition.put(partition, Wrap.runtimeException(() -> JSON.readValue(stateJSON, Topic.State.class)));
            }
        }

        topic.partition = Collections.synchronizedMap(topic.partition);

        Map<String,List<Integer>> brokerPartitionMap = getBrokerPartitionMap(topic);

        ExecutorService exec = Executors.newFixedThreadPool(brokerPartitionMap.size());

        brokerPartitionMap.entrySet().stream().forEach((Map.Entry<String,List<Integer>> entry) -> {
            exec.execute(() -> {
                Broker broker = getBroker(entry.getKey());
                int[] partitions = Ints.toArray(entry.getValue());

                try (Consumer consumer = new Consumer(broker.host, broker.port, 1000, 8 * 1024, me)) {
                    long[] earliest = getOffsets(consumer, name, partitions, EARLIEST);
                    long[] latest = getOffsets(consumer, name, partitions, LATEST);

                    for (int i = 0; i < partitions.length; i++) {
                        State state = topic.partition.get(Integer.toString(partitions[i]));
                        state.earliestOffset = earliest[i];
                        state.latestOffset = latest[i];
                    }
                }
            });
        });

        exec.shutdown();

        // Casting awaitTermination to a boolean to keep Eclipse happy.
        boolean completed = Wrap.runtimeException(() -> (boolean)exec.awaitTermination(10, TimeUnit.SECONDS));

        if (completed) {
            return topic;
        } else {
            return null;
        }
    }

    public Broker getLeader(String topicName, int partition) {
        Topic topic = getTopic(topicName);

        if (topic == null) { return null; }

        State state = topic.partition.get(Integer.toString(partition));

        if (state == null || state.leader == -1) {
            return null;
        } else {
            return getBroker(state.leader);
        }
    }

    public List<String> listBrokers() {
        return zk.list("/brokers/ids");
    }

    public Broker getBroker(int id) { return getBroker(Integer.toString(id)); }

    public Broker getBroker(String id) {
        String brokerJSON = zk.get("/brokers/ids/" + id);

        if (brokerJSON != null && !brokerJSON.isEmpty()) {
            Broker broker = Wrap.runtimeException(() -> JSON.readValue(brokerJSON, Broker.class));
            broker.id = Integer.parseInt(id);

            return broker;
        } else {
            return null;
        }
    }

    private long[] getOffsets(SimpleConsumer consumer, String topic, int[] partitions, long time) {
        long[] offsets = new long[partitions.length];

        Map<TopicAndPartition,PartitionOffsetRequestInfo> requestInfo = new HashMap<>();

        for (int partition : partitions) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time, 1));
        }

        OffsetRequest request = new OffsetRequest(requestInfo, VERSION, me);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        for (int i = 0; i < partitions.length; i++) {
            offsets[i] = response.offsets(topic, partitions[i])[0];
        }

        return offsets;
    }

    public static Map<String,List<Integer>> getBrokerPartitionMap(Topic topic) {
        Map<String,List<Integer>> map = new HashMap<>();

        for (Map.Entry<String,State> entry : topic.partition.entrySet()) {
            String partition = entry.getKey();
            State state = entry.getValue();

            String leader = Integer.toString(state.leader);

            if (!map.containsKey(leader)) { map.put(leader, new ArrayList<>()); }

            map.get(leader).add(Integer.parseInt(partition));
        }

        return map;
    }

    private static class Consumer extends SimpleConsumer implements AutoCloseable {
        public Consumer(String host, int port, int soTimeout, int bufferSize, String clientId) {
            super(host, port, soTimeout, bufferSize, clientId);
        }

        @Override
        public void close() {
            super.close();
        }
    }

    public List<WrappedMessage> fetch(String topicName, int partition, long offset) {
        return fetch(topicName, partition, offset, MAX_MESSAGE_SIZE);
    }

    public List<WrappedMessage> fetch(String topicName, int partition, long offset, int maxBytes) {
        List<WrappedMessage> messages = new ArrayList<>();
        Broker broker = getLeader(topicName, partition);

        if (broker == null || maxBytes <= 0) {
            return null;
        }

        int byteCount = 0;

        if (offset < 0) {
            // offset relative to latest entry
            Topic topic = getTopic(topicName);
            State state = topic.partition.get(Integer.toString(partition));

            if (state == null) { return null; }

            offset = Math.max(0, offset + state.latestOffset);
        }

        try (Consumer consumer = new Consumer(broker.host, broker.port, 1000, maxBytes, me)) {
            FetchRequest request = new FetchRequestBuilder().clientId(me).addFetch(topicName, partition, offset, maxBytes).build();
            FetchResponse response = consumer.fetch(request);
            for (MessageAndOffset msg : response.messageSet(topicName, partition)) {
                ByteBuffer payload = msg.message().payload();
                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                byteCount += bytes.length;
                String string = new String(bytes);

                messages.add(new WrappedMessage(partition, msg.offset(), msg.nextOffset(), topicName, string));

                if (byteCount >= maxBytes) { break; }
            }
        }

        return messages;
    }

    public Map<String,Future<RecordMetadata>> insert(TopicMessageMap map) {
        Map<String,Future<RecordMetadata>> futureMap = new HashMap<>();

        for (Map.Entry<String,List<Message>> entry : map.messages.entrySet()) {
            String topic = entry.getKey();
            List<Message> messages = entry.getValue();

            futureMap.putAll(insert(topic, messages));
        }

        return futureMap;
    }

    public Map<String,Future<RecordMetadata>> insert(String topic, List<Message> messages) {
        Map<String,Future<RecordMetadata>> futureMap = new HashMap<>();

        messages.stream().forEach(message -> {
            futureMap.put(message.id, insert(topic, message));
        });

        return futureMap;
    }

    public Future<RecordMetadata> insert(String topic, Message message) {
        return producer.send(new ProducerRecord<>(topic, Integer.toString(RANDOM.nextInt(Integer.MAX_VALUE)), Util.toJSON(message)));
    }

    @Override
    public void start() throws Exception {
        producer = newProducer();
    }

    @Override
    public void stop() throws Exception {
        producer.close();
    }
}
