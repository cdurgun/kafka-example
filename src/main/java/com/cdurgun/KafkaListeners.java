package com.cdurgun;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListeners {

    @KafkaListener(
            topics = "cdurgun",
            groupId = "groupid",
            containerFactory = "messageFactory"

    )
    void listener(Message data) {
        System.out.println("Listener Received: " + data + " :)");
    }
}
