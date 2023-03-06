package com.example.fileproducer.DTO;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderDTO implements Serializable {
    private Integer totalCost;
    private Integer totalQty;
    private String fileName;

//    private boolean lastRecord;
}
