package com.example.kafkaexamplesspring.datafaker;

import com.example.kafkaexamplesspring.model.PurchaseEvent;
import com.example.kafkaexamplesspring.model.PurchaseStatus;
import com.github.javafaker.Faker;

import java.util.ArrayList;
import java.util.List;

public class PurchaseEventFaker {
    private final UserFaker userFaker = new UserFaker();
    private final ProductFaker productFaker = new ProductFaker();

    public List<PurchaseEvent> generatePurchaseEventsWithInitiatedStatus(int count) {
        List<PurchaseEvent> purchaseEvents = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            purchaseEvents.add(generatePurchaseEventWithInitiatedStatus());
        }

        return purchaseEvents;
    }

    public PurchaseEvent generatePurchaseEventWithInitiatedStatus() {
        return new PurchaseEvent(
                productFaker.generateProduct(),
                userFaker.generateUser(),
                PurchaseStatus.INITIATED
        );
    }
}
