package flinks.connector.clickhouse.applied;

import com.google.common.base.Preconditions;
import flinks.connector.clickhouse.model.ClickHouseSinkCommonParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static flinks.connector.clickhouse.model.ClickHouseSinkConst.MAX_BUFFER_SIZE;
import static flinks.connector.clickhouse.model.ClickHouseSinkConst.TARGET_TABLE_NAME;


public class ClickHouseSinkManager implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSinkManager.class);

    private final ClickHouseWriter clickHouseWriter;
    private final ClickHouseSinkScheduledCheckerAndCleaner clickHouseSinkScheduledCheckerAndCleaner;
    private final ClickHouseSinkCommonParams sinkParams;
    private final List<CompletableFuture<Boolean>> futures = Collections.synchronizedList(new LinkedList<>());
    private volatile boolean isClosed = false;

    public ClickHouseSinkManager(Map<String, String> globalParams) {
        sinkParams = new ClickHouseSinkCommonParams(globalParams);
        clickHouseWriter = new ClickHouseWriter(sinkParams, futures);
        clickHouseSinkScheduledCheckerAndCleaner = new ClickHouseSinkScheduledCheckerAndCleaner(sinkParams, futures);
        logger.info("Build sink writer's manager. params = {}", sinkParams.toString());
    }

    public Sink buildSink(Properties localProperties) {
        String targetTable = localProperties.getProperty(TARGET_TABLE_NAME);
        int maxFlushBufferSize = Integer.parseInt(localProperties.getProperty(MAX_BUFFER_SIZE));

        return buildSink(targetTable, maxFlushBufferSize);
    }

    public Sink buildSink(String targetTable, int maxBufferSize) {
        Preconditions.checkNotNull(clickHouseSinkScheduledCheckerAndCleaner);
        Preconditions.checkNotNull(clickHouseWriter);

        ClickHouseSinkBuffer clickHouseSinkBuffer = ClickHouseSinkBuffer.Builder
                .aClickHouseSinkBuffer()
                .withTargetTable(targetTable)
                .withMaxFlushBufferSize(maxBufferSize)
                .withTimeoutSec(sinkParams.getTimeout())
                .withFutures(futures)
                .build(clickHouseWriter);

        clickHouseSinkScheduledCheckerAndCleaner.addSinkBuffer(clickHouseSinkBuffer);

        if (sinkParams.isIgnoringClickHouseSendingExceptionEnabled()) {
            return new UnexceptionableSink(clickHouseSinkBuffer);
        } else {
            return new ExceptionsThrowableSink(clickHouseSinkBuffer);
        }

    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() throws Exception {
        logger.info("ClickHouse sink manager is shutting down.");
        clickHouseSinkScheduledCheckerAndCleaner.close();
        clickHouseWriter.close();
        isClosed = true;
        logger.info("ClickHouse sink manager shutdown complete.");
    }
}
