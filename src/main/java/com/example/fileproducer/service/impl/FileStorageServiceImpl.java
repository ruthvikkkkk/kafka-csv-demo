package com.example.fileproducer.service.impl;

import com.example.entities.FileEntity;
import com.example.fileproducer.DTO.OrderDTO;
import com.example.fileproducer.service.FileStorageService;
import com.example.repository.FileRepository;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Service
public class FileStorageServiceImpl implements FileStorageService {

    //declarations and stuff
    private final Path root = Paths.get("temp-file-dir");
    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    FileRepository fileRepository;

    private static final String TOPIC = "OrderTopic";

    @Override
    public void init() {
        try{
            Files.createDirectories(root);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public String save(MultipartFile file) {
        try{
            FileEntity fileEntity = FileEntity.builder()
                    .fileName(file.getOriginalFilename())
                    .status("uploading")
                    .build();

            if(!fileRepository.existsById(fileEntity.getFileName())) {

                Path path = Paths.get("temp-file-dir/"+file.getOriginalFilename());
                Files.deleteIfExists(path);
                File myFile = new File(Files.write(path, file.getBytes()).toUri());

                fileRepository.save(fileEntity);
                saveFile(myFile);
                return "saved!";
            }else {
                return "file already saved!";
            }
        }catch (Exception e){
            e.printStackTrace();
            return "could not save!";
        }
    }

    @Override
    public String saveFile(File file) {
        final long[] offset = new long[1];
        try{

            CSVReader csvReader = new CSVReader(new FileReader(file));
            CSVReader csvReader2 = new CSVReader(new FileReader(file));
            csvReader.readNext();

            int count = 0;
            String[] values = csvReader.readNext();
            Integer totalSize = csvReader2.readAll().size()-1;
            //kafkaTemplate.send(TOPIC, totalSize);
            do {
                count++;
                OrderDTO orderDTO = OrderDTO.builder()
                        .totalCost(Integer.parseInt(values[0]))
                        .totalQty(Integer.parseInt(values[1]))
                        .fileName("")
                        .build();

                if(!(count == totalSize)){

                    int finalCount = count;
                    kafkaTemplate.send(TOPIC, orderDTO)
                            .addCallback(new ListenableFutureCallback() {
                                @Override
                                public void onFailure(Throwable ex) {

                                }

                                @Override
                                public void onSuccess(Object result) {
                                    //offset[0] = ((SendResult) result).getRecordMetadata().offset();
                                    System.out.println(finalCount);
                                }
                            });

                }else{

                    orderDTO.setFileName(file.getName());
                    kafkaTemplate.send(TOPIC, 4, "last record", orderDTO)
                            .addCallback(new ListenableFutureCallback() {
                                @Override
                                public void onFailure(Throwable ex) {

                                }

                                @Override
                                public void onSuccess(Object result) {
                                    System.out.println("all messages pushed!");
                                }
                            });
                }

//                ListenableFuture<SendResult> futureResult = kafkaTemplate.send(TOPIC, orderDTO);
//                futureResult.addCallback(new ListenableFutureCallback() {
//                    @Override
//                    public void onFailure(Throwable ex) {
//                        ex.printStackTrace();
//                    }
//
//                    @Override
//                    public void onSuccess(Object result) {
//                        offset[0] = ((SendResult) result).getRecordMetadata().offset();
//                        System.out.println(((SendResult) result).getRecordMetadata().offset());
//                        if(offset[0] == totalSize){
//                            System.out.println("All messages pushed to kafka");
//                            //fileRepository.updateStatus(file.getName());
//                        }else{
//                            System.out.println();
//                        }
//                    }
//                });
            }while((values = csvReader.readNext()) != null);
            return "saved file!";
        }catch (Exception e){
            e.printStackTrace();
            return "could not save!";
        }
    }

    @Override
    public String createFile() {
        synchronized (FileStorageServiceImpl.class) {
            List<String[]> orderList = new ArrayList<>();

            orderList.add(new String[]{"totalCost", "totalQty"});
            Random random = new Random();
            for (int i = 0; i < 1000; i++) {
                orderList.add(new String[]{String.valueOf(random.nextInt(20000)), String.valueOf(random.nextInt(20))});
            }
            int numDir = new File("/Users/jakkaruthvikkumar/Documents/ordersDir").listFiles().length;

            File file = new File("/Users/jakkaruthvikkumar/Documents/ordersDir/orders" + numDir + ".csv");

            try {
                FileWriter fileWriter = new FileWriter(file);
                CSVWriter csvWriter = new CSVWriter(fileWriter);

                csvWriter.writeAll(orderList);
                csvWriter.close();

                FileReader reader = new FileReader(file);

                return "done";
            } catch (Exception e) {
                e.printStackTrace();
                return "fail";
            }
        }
    }

}
