package distinct.bitmap;

import org.apache.flink.table.functions.AggregateFunction;

public class PreciseDistinct extends AggregateFunction<Long, PreciseAccumulator> {

    @Override public PreciseAccumulator createAccumulator() {
        return new PreciseAccumulator();
    }

    public void accumulate(PreciseAccumulator accumulator,long id){
        accumulator.add(id);
    }

    @Override public Long getValue(PreciseAccumulator accumulator) {
        return accumulator.getCardinality();
    }
}
