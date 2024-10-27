package com.example.kafkaexamplesspring.producer;

import com.example.kafkaexamplesspring.datafaker.PurchaseEventFaker;
import com.example.kafkaexamplesspring.model.PurchaseEvent;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Simple class with simple purpose: constantly publish new purchase events
 */
@Slf4j
public class PurchaseEventFakerProducerThread implements Runnable {
    private final PurchaseEventProducer purchaseEventProducer;
    private final PurchaseEventFaker purchaseEventFaker;
    private final Long sleepInterval;
    private final static Long sleepIntervalDefault = 1000L;
    private final int purchaseEventsPerBatch;
    private final static int purchaseEventsPerBatchDefault = 10;

    public PurchaseEventFakerProducerThread(String broker) {
        this(broker, sleepIntervalDefault, purchaseEventsPerBatchDefault);
    }

    public PurchaseEventFakerProducerThread(String broker, Long sleepInterval, int purchaseEventsPerBatch) {
        this(new PurchaseEventProducer(broker), new PurchaseEventFaker(), sleepInterval, purchaseEventsPerBatch);
    }

    public PurchaseEventFakerProducerThread(PurchaseEventProducer purchaseEventProducer, PurchaseEventFaker purchaseEventFaker, Long sleepInterval, int purchaseEventsPerBatch) {
        this.purchaseEventProducer = purchaseEventProducer;
        this.purchaseEventFaker = purchaseEventFaker;
        this.sleepInterval = sleepInterval;
        this.purchaseEventsPerBatch = purchaseEventsPerBatch;
    }

    @SneakyThrows
    @Override
    public void run() {
        while (true) {
            List<PurchaseEvent> purchaseEvents = purchaseEventFaker.generatePurchaseEventsWithInitiatedStatus(purchaseEventsPerBatch);
            purchaseEvents.forEach(purchaseEvent -> {
                try {
                    purchaseEventProducer.sendEventSync(purchaseEvent.getUser().getType().name(), purchaseEvent);
                } catch (Exception e) {
                    log.error("unable to send event", e);
                    throw new RuntimeException(e);
                }
            });
            Thread.sleep(sleepInterval);
        }
    }
}
