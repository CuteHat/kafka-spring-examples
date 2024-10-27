package com.example.kafkaexamplesspring.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.math.BigDecimal;

// Dummy class to represent user
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Getter
public class User {
    private Long id;
    private String name;
    private UserType Type;
    private BigDecimal balance;
}
