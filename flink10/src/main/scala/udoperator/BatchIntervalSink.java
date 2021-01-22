package udoperator;

import java.util.List;

public class BatchIntervalSink extends CommonSinkOperator<String> {

    public BatchIntervalSink(int batchSize, long interval) {
        super(batchSize, interval);
    }

    @Override
    public void saveRecords(List<String> datas) {
        datas.forEach(System.out::println);
    }
}
