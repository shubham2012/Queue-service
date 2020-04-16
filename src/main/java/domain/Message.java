package domain;

import lombok.Data;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
public class Message {

    private UUID id;
    private String message;
    private String key;
    private LocalDateTime createdOn;

    public Message(String key, String message) {
        this.id = UUID.randomUUID();
        this.key = key;
        this.message = message;
        this.createdOn = LocalDateTime.now();
    }
}
