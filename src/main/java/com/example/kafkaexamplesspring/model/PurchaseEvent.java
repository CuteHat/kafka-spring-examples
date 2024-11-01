package com.example.kafkaexamplesspring.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@ToString
@Getter
public class PurchaseEvent {
    private Product product; // what has been purchased
    private User user; // who did
    private PurchaseStatus status;
}