package com.example.kafkaexamplesspring.datafaker;

import com.example.kafkaexamplesspring.model.User;
import com.example.kafkaexamplesspring.model.UserType;
import com.github.javafaker.Faker;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class UserFaker {
    private static final Faker faker = new Faker();

    public List<User> generateUsers(int count) {
        List<User> users = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            users.add(generateUser());
        }

        return users;
    }

    public User generateUser() {
        return new User(
                faker.random().nextLong(),
                faker.name().fullName(),
                faker.options().option(UserType.class),
                new BigDecimal(faker.commerce().price())
        );
    }
}
