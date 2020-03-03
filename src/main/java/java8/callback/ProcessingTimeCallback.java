package java8.callback;

public interface ProcessingTimeCallback {

    void onProcessingTime(long timestamp) throws Exception;
}
