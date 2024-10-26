package com.example.kafkaexamplesspring.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

// Dummy class to represent user
@AllArgsConstructor
@Getter
public class User {
    private Long id;
    private String name;
    private UserType Type;

    @Override
    public String toString() {
        return "User{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", Type=" + Type +
                '}';
    }
}
