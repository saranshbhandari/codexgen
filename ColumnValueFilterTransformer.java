package com.test.dataflowengine.processors.taskprocessorshelpers.transformers;

import com.test.dataflowengine.models.tasksettings.subsettings.transformations.ColumnValueFilterSettings;
import com.test.dataflowengine.models.tasksettings.subsettings.transformations.Transformation;
import com.test.dataflowengine.processors.taskprocessorshelpers.transformers.filter.ColumnValueFilterPredicateCompiler;
import com.test.dataflowengine.processors.taskprocessorshelpers.transformers.filter.RowPredicate;
import com.test.dataflowengine.variablemanager.variablestore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Service
public class ColumnValueFilterTransformer implements RecordTransformer {

    private static final Logger log = LoggerFactory.getLogger(ColumnValueFilterTransformer.class);

    @Override
    public void transformBatchInPlace(Transformation transformation,
                                     List<Map<String, Object>> batch,
                                     variablestore varstore) {

        int rowsIn = batch == null ? 0 : batch.size();
        long start = TransformerMetrics.startNs();

        try {
            if (batch == null || batch.isEmpty()) {
                TransformerMetrics.log(log, "COLUMN_VALUE_FILTER", rowsIn, rowsIn, 0, start);
                return;
            }

            ColumnValueFilterSettings settings =
                    transformation.getSettingsAsType(ColumnValueFilterSettings.class);

            // ⚡ short-circuit: no groups => nothing to filter
            if (settings == null || settings.getGroups() == null || settings.getGroups().isEmpty()) {
                TransformerMetrics.log(log, "COLUMN_VALUE_FILTER", rowsIn, rowsIn, 0, start);
                return;
            }

            RowPredicate predicate = ColumnValueFilterPredicateCompiler.compile(settings);

            int removed = 0;
            Iterator<Map<String, Object>> it = batch.iterator();
            while (it.hasNext()) {
                Map<String, Object> row = it.next();
                if (!predicate.test(row, varstore)) {
                    it.remove();
                    removed++;
                }
            }

            TransformerMetrics.log(log, "COLUMN_VALUE_FILTER", rowsIn, batch.size(), removed, start);
        } catch (Exception ex) {
            // still log metrics-like info for observability
            log.error("COLUMN_VALUE_FILTER failed. rowsIn={}", rowsIn, ex);
            throw ex;
        }
    }
}
