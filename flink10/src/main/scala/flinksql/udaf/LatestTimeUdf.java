package flinksql.udaf;

import org.apache.flink.table.functions.AggregateFunction;

//聚合函数  ===>  多对一
public class LatestTimeUdf extends AggregateFunction<Integer, TimeAndStatus> {


    /***
     * createAccumulator 表示创建一个中间结果数据，由于是以设备为维度那么对于每一个设备都会调用一次该方法；
     * @return
     */
    @Override
    public TimeAndStatus createAccumulator() {
        return new TimeAndStatus();

    }

    /***
     * accumulate 表示将流入的数据聚合到createAccumulator创建的中间结果数据中，第一个参数表示的是ACC类型的中间结果数据，
     * 其他的表示自定义函数的入参，该方法可以接受不同类型、个数的入参，也就是该方法可以被重载，Flink会自动根据类型提取找到合适的方法。
     * 在这里接受的是Integer类型的设备状态与long类型的时间戳，处理逻辑就是与中间结果数据时间进行比较，
     * 如果比其大则将流入的时间与设备状态更新到中间结果中。
     * 另外在做一点补充accumulate的调用是相同维度的调用，即acc每次都是该维度的中间结果数据，入参也是该维度的数据；
     * @param acc
     * @param status
     * @param time
     */
    public void accumulate(TimeAndStatus acc, Integer status, Long time) {
        if (time > acc.getTimes()) {
            acc.setStatus(status);
            acc.setTimes(time);
        }
    }

    /***
     * getValue 表示一次返回的结果，结果数据从acc中获取，这个方法在accumulate之后被调用。
     * @param timeAndStatus
     * @return
     */
    @Override
    public Integer getValue(TimeAndStatus timeAndStatus) {
        return timeAndStatus.getStatus();
    }
}
