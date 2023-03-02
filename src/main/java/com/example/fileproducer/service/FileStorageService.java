package com.example.fileproducer.service;

import org.springframework.web.multipart.MultipartFile;

import java.io.File;

public interface FileStorageService {
    void init();
    String save(MultipartFile file);

    String saveFile(File file);

    String createFile();
}
