package com.example.kafkaexamplesspring.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@ToString
@Getter
public class Product {
    private Long id;
    private String name;
    private BigDecimal price;
}
