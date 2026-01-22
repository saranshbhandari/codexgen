package com.test.dataflowengine.processors.taskprocessorshelpers.transformers;

import org.slf4j.Logger;

public final class TransformerMetrics {

    private TransformerMetrics() {}

    public static long startNs() {
        return System.nanoTime();
    }

    public static void log(Logger log,
                           String transformerName,
                           int rowsIn,
                           int rowsOut,
                           int rowsRemoved,
                           long startNs) {

        long tookMs = (System.nanoTime() - startNs) / 1_000_000L;

        // MDC (wfid/wfexecutionid/childwfid) is automatically included by your logging pattern if configured.
        log.info("TransformerMetrics name={} rowsIn={} rowsOut={} removed={} tookMs={}",
                transformerName, rowsIn, rowsOut, rowsRemoved, tookMs);
    }
}
