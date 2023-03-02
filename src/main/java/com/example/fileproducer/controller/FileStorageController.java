package com.example.fileproducer.controller;

import com.example.fileproducer.service.FileStorageService;
import com.google.gson.Gson;
import io.swagger.v3.oas.annotations.Operation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@RequestMapping("")
public class FileStorageController {

    @Autowired
    FileStorageService storageService;

    Gson gson = new Gson();
    @PostMapping(value = "/upload", consumes = {"multipart/form-data"})
    @Operation(summary = "Upload a single File")
    public ResponseEntity<String> upload(@RequestPart(required = true) MultipartFile file) throws IOException {

        return new ResponseEntity<>(storageService.save(file), HttpStatus.OK);
    }


    @GetMapping("/createAndSaveFile")
    public ResponseEntity<String> create(){
        return new ResponseEntity<>(storageService.createFile(), HttpStatus.OK);
    }
}
