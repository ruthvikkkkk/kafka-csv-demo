package com.example.fileproducer.service.impl;

import com.example.entities.FileEntity;
import com.example.fileproducer.DTO.OrderDTO;
import com.example.fileproducer.service.FileStorageService;
import com.example.repository.FileRepository;
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
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
    KStream kStream;

    private final Path root = Paths.get("temp-file-dir");
    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    FileRepository fileRepository;
    int count = 0, totalSize = 0;
    String fileName = "";

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
    public String saveFile(File file)        {
        try{
            fileName = file.getName();
            count = 0;
            totalSize = 0;
            CSVReader csvReader = new CSVReader(new FileReader(file));

            List<String[]> stringList = csvReader.readAll();
            stringList.remove(0);
            totalSize = stringList.size() - 1;

            stringList.forEach(strings -> {
                count++;
                OrderDTO orderDTO = new OrderDTO(Integer.parseInt(strings[0]), Integer.parseInt(strings[1]), "");
                publishMessage(orderDTO);
            });
            return "saved file!";
        }catch (Exception e){
            e.printStackTrace();
            return "could not save!";
        }
    }

    private void publishMessage(OrderDTO orderDTO) {
        if(!(count == totalSize)){

            kafkaTemplate.send(TOPIC, orderDTO);
            System.out.println(count);
        }else{
            orderDTO.setFileName(fileName);
            kafkaTemplate.send(TOPIC, orderDTO);
            System.out.println("last message published!");
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

                return "done";
            } catch (Exception e) {
                e.printStackTrace();
                return "fail";
            }
        }
    }
}