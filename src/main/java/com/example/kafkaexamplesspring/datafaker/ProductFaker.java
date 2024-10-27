package com.example.kafkaexamplesspring.datafaker;

//import com.github.

import com.example.kafkaexamplesspring.model.Product;
import com.github.javafaker.Faker;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class ProductFaker {
    private static final Faker faker = new Faker();

    public List<Product> generateProducts(int count) {
        List<Product> products = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            products.add(generateProduct());
        }

        return products;
    }

    public Product generateProduct() {
        return new Product(
                faker.random().nextLong(),
                faker.commerce().productName(),
                new BigDecimal(faker.commerce().price())
        );
    }
}
