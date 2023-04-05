package com.example.fileconsumer.consumer;

import com.example.entities.OrderEntity;
import com.example.fileproducer.DTO.OrderDTO;
import com.example.repository.FileRepository;
import com.example.repository.OrderRepository;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.transaction.Transactional;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Component
public class MyKafkaConsumer {

    //declarations and stuff
    @Autowired
    OrderRepository orderRepository;
    int i = 1;

    @Autowired
    FileRepository fileRepository;

    int count;
    long numberOfMessagesReceived;
    long consumerOffsetSum = 0;
    long producerOffsetSum = 0;

    KafkaProperties kafkaProperties = new KafkaProperties();
    AdminClient adminClient = AdminClient.create(kafkaProperties.buildAdminProperties());

    public MyKafkaConsumer(){
//        updateProducerOffsetSum();
//        updateConsumerOffsetSum();
        this.numberOfMessagesReceived = 0;
    }
    @PostConstruct
    void init(){
        this.count = getRepoCount();
    }

    private int getRepoCount(){
        return orderRepository.findAll().size();
    }

//    @KafkaListener(topicPartitions = {
//            @TopicPartition(topic = "OrderTopic", partitions = {"0"})
//    }, groupId = "my_consumer")
//
//    @KafkaListener(topics = "OrderTopic", groupId = "my_consumer")
//    @Transactional
//    public void orderHandlerZero(OrderDTO order){
//        System.out.println("handler0 - " + this.numberOfMessagesReceived++);
//
//        synchronized (MyKafkaConsumer.class) {
//            OrderEntity orderEntity = OrderEntity.builder()
//                    .totalQty(order.getTotalQty())
//                    .totalCost(order.getTotalCost())
//                    .fileName(order.getFileName())
//                            .build();
//            orderRepository.save(orderEntity);
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//
//            if(!orderEntity.getFileName().equals("")){
//                Optional<FileEntity> optionalFile = fileRepository.findById(orderEntity.getFileName());
//                if(optionalFile.isPresent()) {
//                    fileRepository.updateStatus(orderEntity.getFileName());
//
//                    System.out.println("handler0\n" + "prev - " + this.count + "\nnow - " + orderRepository.count());
//                    System.out.println("Finished");
//                    this.count = getRepoCount();
//                }
//            }
//        }
//    }
//
////    @KafkaListener(topicPartitions = {
////            @TopicPartition(topic = "OrderTopic", partitions = {"1"})
////    }, groupId = "my_consumer")
//
//    @KafkaListener(topics = "OrderTopic", groupId = "my_consumer")
//    @Transactional
//    public void orderHandlerOne(OrderDTO order){
//
//        System.out.println("handler1 - " + this.numberOfMessagesReceived++);
//
//        synchronized (MyKafkaConsumer.class) {
//            OrderEntity orderEntity = OrderEntity.builder()
//                    .totalQty(order.getTotalQty())
//                    .totalCost(order.getTotalCost())
//                    .fileName(order.getFileName())
//                    .build();
//            orderRepository.save(orderEntity);
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//
//            if(!orderEntity.getFileName().equals("")){
//                Optional<FileEntity> optionalFile = fileRepository.findById(orderEntity.getFileName());
//                if(optionalFile.isPresent()) {
//                    fileRepository.updateStatus(orderEntity.getFileName());
//
//                    System.out.println("handler1\n" + "prev - " + this.count + "\nnow - " + orderRepository.count());
//                    System.out.println("Finished");
//                    this.count = getRepoCount();
//                }
//            }
//        }
//    }
//
////    @KafkaListener(topicPartitions = {
////            @TopicPartition(topic = "OrderTopic", partitions = {"2"})
////    }, groupId = "my_consumer")
//
//    @KafkaListener(topics = "OrderTopic", groupId = "my_consumer")
//    @Transactional
//    public void orderHandlerTwo(OrderDTO order){
//
//        System.out.println("handler2 - " + this.numberOfMessagesReceived++);
//
//        synchronized (MyKafkaConsumer.class) {
//            OrderEntity orderEntity = OrderEntity.builder()
//                    .totalQty(order.getTotalQty())
//                    .totalCost(order.getTotalCost())
//                    .fileName(order.getFileName())
//                    .build();
//            orderRepository.save(orderEntity);
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//
//            if(!orderEntity.getFileName().equals("")){
//                Optional<FileEntity> optionalFile = fileRepository.findById(orderEntity.getFileName());
//                if(optionalFile.isPresent()) {
//                    fileRepository.updateStatus(orderEntity.getFileName());
//
//                    System.out.println("handler2\n" + "prev - " + this.count + "\nnow - " + orderRepository.count());
//                    System.out.println("Finished");
//                    this.count = getRepoCount();
//                }
//            }
//        }
//    }
//
////    @KafkaListener(topicPartitions = {
////            @TopicPartition(topic = "OrderTopic", partitions = {"3"})
////    }, groupId = "my_consumer")

//    @KafkaListener(topicPartitions = {
//            @TopicPartition(topic = "OrderTopic", partitions = {"4"})
//    }, groupId = "my_consumer")
    @KafkaListener(topics = "OrderTopic", groupId = "my_consumer")
    @Transactional
    public void orderHandlerFour(OrderDTO order){

            OrderEntity orderEntity = OrderEntity.builder()
                    .totalQty(order.getTotalQty())
                    .totalCost(order.getTotalCost())
                    .fileName(order.getFileName())
                    .build();

            orderRepository.save(orderEntity);

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            updateStatus(order.getFileName());

//            if(!orderEntity.getFileName().equals("")){
//                Optional<FileEntity> optionalFile = fileRepository.findById(orderEntity.getFileName());
//                if(optionalFile.isPresent()) {
//                    fileRepository.updateStatus(orderEntity.getFileName());
//
//                    System.out.println("handler4\n" + "prev - " + this.count + "\nnow - " + orderRepository.count());
//                    System.out.println("Finished");
//                    this.count = getRepoCount();
//                }
//            }

    }

    public void updateProducerOffsetSum(){
        try {
            List<Integer> partitions = adminClient.describeTopics(Collections.singletonList("OrderTopic"))
                    .topicNameValues()
                    .get("OrderTopic")
                    .get()
                    .partitions()
                    .stream()
                    .map(TopicPartitionInfo::partition)
                    .collect(Collectors.toList());

            this.producerOffsetSum =partitions.stream()
                    .map(integer -> new TopicPartition("OrderTopic", integer))
                    .flatMap(topicPartition -> {
                        Map<TopicPartition, OffsetSpec> specHashMap = new HashMap<>();
                        specHashMap.put((topicPartition), new OffsetSpec());

                        try {
                            return adminClient.listOffsets(specHashMap)
                                    .all()
                                    .get()
                                    .values()
                                    .stream()
                                    .map(ListOffsetsResult.ListOffsetsResultInfo::offset);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .reduce(0L, (a, b) -> a + b);

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateStatus(String name){
        updateProducerOffsetSum();
        updateConsumerOffsetSum();
        this.numberOfMessagesReceived++;
        //System.out.println(i++);

        System.out.println("consumer - " + (this.numberOfMessagesReceived + this.count) + "\nproducer - " + this.producerOffsetSum);
        System.out.println();

        if((this.numberOfMessagesReceived + this.count) == this.producerOffsetSum){
            System.out.println("all messages consumed!");
            fileRepository.updateStatus(name);
            this.count = getRepoCount();
        }
    }

    public void updateConsumerOffsetSum(){
        ListConsumerGroupOffsetsResult consumerGroupOffsetsResult = adminClient.listConsumerGroupOffsets("my_consumer");
        try {
            Map<TopicPartition, OffsetAndMetadata> consumerData =  consumerGroupOffsetsResult.partitionsToOffsetAndMetadata().get();
            List<OffsetAndMetadata> offsetAndMetadataList = new ArrayList<>(consumerData.values());
            this.consumerOffsetSum = offsetAndMetadataList.stream()
                    .map(offsetAndMetadata -> offsetAndMetadata.offset())
                    .reduce(0L, (a, b) -> a+b);
//            for (OffsetAndMetadata offsetAndMetadata : offsetAndMetadataList) {
//                this.consumerOffsetSum += offsetAndMetadata.offset();
//            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @KafkaListener(topics = "OrderTopic", groupId = "my_consumer")
    @Transactional
    public void orderHandlerThree(OrderDTO order){

        //System.out.println("handler3 - " + this.numberOfMessagesReceived++);

        synchronized (MyKafkaConsumer.class) {
            OrderEntity orderEntity = OrderEntity.builder()
                    .totalQty(order.getTotalQty())
                    .totalCost(order.getTotalCost())
                    .fileName(order.getFileName())
                    .build();
            orderRepository.save(orderEntity);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            updateStatus(order.getFileName());

//            if(!orderEntity.getFileName().equals("")){
//                Optional<FileEntity> optionalFile = fileRepository.findById(orderEntity.getFileName());
//                if(optionalFile.isPresent()) {
//                    fileRepository.updateStatus(orderEntity.getFileName());
//
//                    System.out.println("handler3\n" + "prev - " + this.count + "\nnow - " + orderRepository.count());
//                    System.out.println("Finished");
//                    this.count = getRepoCount();
//                }
//            }
        }
    }
}
