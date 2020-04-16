package exception;

public class QueueException extends Exception {

    public QueueException(String message, String... args) {
        super(message + " " +String.join(" ", args));
    }
}
