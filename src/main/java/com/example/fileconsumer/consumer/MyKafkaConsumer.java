package com.example.fileconsumer.consumer;

import com.example.entities.FileEntity;
import com.example.entities.OrderEntity;
import com.example.fileproducer.DTO.OrderDTO;
import com.example.repository.FileRepository;
import com.example.repository.OrderRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.transaction.Transactional;
import java.util.Optional;

@Component

public class MyKafkaConsumer {

    //declarations and stuff
    @Autowired
    OrderRepository orderRepository;

    @Autowired
    FileRepository fileRepository;

    int count;
    int numberOfMessagesReceived;
    int messagesToReceive;

    @Autowired
    KafkaListenerEndpointRegistry registry;

    public MyKafkaConsumer(){
        //this.count = orderRepository.findAll().size();
        this.numberOfMessagesReceived = 0;
        this.messagesToReceive = 0;
    }
    @PostConstruct
    void init(){
        this.count = getRepoCount();
        //this.i = 0;
    }

    private int getRepoCount(){
        return orderRepository.findAll().size();
    }

    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = "OrderTopic", partitions = {"0"})
    }, groupId = "my_consumer")
    @Transactional
    public void orderHandlerZero(OrderDTO order){

        System.out.println("handler0 - " + this.numberOfMessagesReceived++);

        ObjectMapper mapper = new ObjectMapper();
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

//            if((getRepoCount()) == (this.messagesToReceive + this.count)){
//                    System.out.println("prev - " + this.count + "\nnow -" + orderRepository.count());
//                    System.out.println("Finished");
//                    this.count = getRepoCount();
//                    List<FileEntity> fileEntityList = fileRepository.findAll().stream()
//                            .peek(fileEntity -> fileEntity.setStatus("finished uploading!"))
//                            .map(fileRepository::save)
//                            .collect(Collectors.toList());
//
//            }
        }
    }

//handler two
    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = "OrderTopic", partitions = {"1"})
    }, groupId = "my_consumer")
    @Transactional
    public void orderHandlerOne(OrderDTO order){

        System.out.println("handler1 - " + this.numberOfMessagesReceived++);

        ObjectMapper mapper = new ObjectMapper();
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

//            if((getRepoCount()) == (this.messagesToReceive + this.count)){
//                    System.out.println("prev - " + this.count + "\nnow -" + orderRepository.count());
//                    System.out.println("Finished");
//                    this.count = getRepoCount();
//                    List<FileEntity> fileEntityList = fileRepository.findAll().stream()
//                            .peek(fileEntity -> fileEntity.setStatus("finished uploading!"))
//                            .map(fileRepository::save)
//                            .collect(Collectors.toList());
//
//            }
        }
    }

    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = "OrderTopic", partitions = {"2"})
    }, groupId = "my_consumer")
    @Transactional
    public void orderHandlerTwo(OrderDTO order){

        System.out.println("handler2 - " + this.numberOfMessagesReceived++);

        ObjectMapper mapper = new ObjectMapper();
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

//            if((getRepoCount()) == (this.messagesToReceive + this.count)){
//                    System.out.println("prev - " + this.count + "\nnow -" + orderRepository.count());
//                    System.out.println("Finished");
//                    this.count = getRepoCount();
//                    List<FileEntity> fileEntityList = fileRepository.findAll().stream()
//                            .peek(fileEntity -> fileEntity.setStatus("finished uploading!"))
//                            .map(fileRepository::save)
//                            .collect(Collectors.toList());
//
//            }
        }
    }

    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = "OrderTopic", partitions = {"3"})
    }, groupId = "my_consumer")
    @Transactional
    public void orderHandlerThree(OrderDTO order){

        System.out.println("handler3 - " + this.numberOfMessagesReceived++);

        ObjectMapper mapper = new ObjectMapper();
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

//            if((getRepoCount()) == (this.messagesToReceive + this.count)){
//                    System.out.println("prev - " + this.count + "\nnow -" + orderRepository.count());
//                    System.out.println("Finished");
//                    this.count = getRepoCount();
//                    List<FileEntity> fileEntityList = fileRepository.findAll().stream()
//                            .peek(fileEntity -> fileEntity.setStatus("finished uploading!"))
//                            .map(fileRepository::save)
//                            .collect(Collectors.toList());
//
//            }
        }
    }

    @KafkaListener(topicPartitions = {
            @TopicPartition(topic = "OrderTopic", partitions = {"4"})
    }, groupId = "my_consumer")
    @Transactional
    public void orderHandlerFour(OrderDTO order){

        System.out.println("handler4 - " + this.numberOfMessagesReceived++);

        ObjectMapper mapper = new ObjectMapper();
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

            if(!orderEntity.getFileName().equals("")){
                Optional<FileEntity> optionalFile = fileRepository.findById(orderEntity.getFileName());
                if(optionalFile.isPresent()) {
                    fileRepository.updateStatus(orderEntity.getFileName());

                    System.out.println("handler4\n" + "prev - " + this.count + "\nnow - " + orderRepository.count());
                    System.out.println("Finished");
                    this.count = getRepoCount();
                }
            }

//            if((getRepoCount()) == (this.messagesToReceive + this.count)){
//                    System.out.println("prev - " + this.count + "\nnow -" + orderRepository.count());
//                    System.out.println("Finished");
//                    this.count = getRepoCount();
//                    List<FileEntity> fileEntityList = fileRepository.findAll().stream()
//                            .peek(fileEntity -> fileEntity.setStatus("finished uploading!"))
//                            .map(fileRepository::save)
//                            .collect(Collectors.toList());
//
//            }
        }
    }
}
