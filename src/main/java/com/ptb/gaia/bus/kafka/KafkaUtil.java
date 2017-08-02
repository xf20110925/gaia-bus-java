package com.ptb.gaia.bus.kafka;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.zookeeper.ZooKeeper;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by eric on 16/4/23.
 */
class KafkaUtil {
    private static List<Integer> getPartitions(String zookeepers, String topic) throws Exception {
        ZooKeeper zk = new ZooKeeper(zookeepers, 500000, watchedEvent -> {

        });
        try {
            return zk.getChildren(String.format("/brokers/topics/%s/partitions", topic), false).stream().map(k -> {
                return Integer.valueOf(k);
            }).collect(Collectors.toList());
        } finally {
            if (zk != null) {
                zk.close();
            }
        }
    }

    private static long getPartionsGroupOffset(String zookeepers, String group, String topic, int partition) throws Exception {
        ZooKeeper zk = new ZooKeeper(zookeepers, 500000, watchedEvent -> {

        });
        try {
            byte[] data = zk.getData(String.format("/consumers/%s/offsets/%s/%d", group, topic, partition), false, null);
            return Long.valueOf(new String(data));
        } finally {
            if (zk != null) {
                zk.close();
            }
        }
    }


    private static PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : a_seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + a_topic
                        + ", " + a_partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        return returnMetaData;
    }


    private static long getLastOffset(String host, int port, String topic, int partition,
                                      long whichTime, String clientName) {
        SimpleConsumer consumer = new SimpleConsumer(host, port, 100000, 64 * 1024, "getoffset");
        try {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
            Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
            requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
            kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                    requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
            OffsetResponse response = consumer.getOffsetsBefore(request);

            if (response.hasError()) {
                System.out.println(String.format("Error fetching data Offset Data the Broker. Reason: %s topic[%s] partition[%s]", response.errorCode(topic, partition),topic,partition));
                return 0;
            }
            long[] offsets = response.offsets(topic, partition);
            return offsets[0];
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public static long getRestMessge(String brokers, String zookeepers, int brokerPort, String topic, String group) throws Exception {
        List<Integer> partitions = getPartitions(zookeepers, topic);
        String[] split1 = brokers.replaceAll("\\:\\d*[^\\,]*", "").split(",");
        List<String> bs = Arrays.asList(split1);

        long restMessages = partitions.stream().map(p -> {
            String host = findLeader(bs, brokerPort, topic, p).leader().host();
            long getoffset = getLastOffset(host, brokerPort, topic, p, -1L, "getoffset");
            long useroffset = 0;
            try {
                useroffset = getPartionsGroupOffset(zookeepers, group, topic, p);
            } catch (Exception e) {
            }
            return getoffset - useroffset;
        }).reduce((v1, v2) -> v1 + v2).get();
        return restMessages;
    }

    public static void main(String[] args) throws Exception {
        while(true) {
            long gaia = getRestMessge("192.168.5.35:9092,192.168.5.36:9092,192.168.5.37:9092", "192.168.5.35:2181,192.168.5.36:2181,192.168.5.37:2181", 9092, "uranus-server", "gaia");
            System.out.println(gaia);
        }

    }
}
