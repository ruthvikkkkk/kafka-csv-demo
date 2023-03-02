package com.example.entities;

import lombok.*;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Table(name = "Files")
@Entity
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FileEntity {

    @Id
    private String fileName;
    private String status;
}