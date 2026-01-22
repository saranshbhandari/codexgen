package com.test.dataflowengine.processors.taskprocessorshelpers.transformers;

import com.test.dataflowengine.models.tasksettings.subsettings.transformations.CustomColumnGeneratorSettings;
import com.test.dataflowengine.models.tasksettings.subsettings.transformations.Transformation;
import com.test.dataflowengine.variablemanager.variablestore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class CustomValueGenerationTransformer implements RecordTransformer {

    private static final Logger log = LoggerFactory.getLogger(CustomValueGenerationTransformer.class);
    private static final String PREFIX = "CUSTOMCOLUMN_";

    @Override
    public void transformBatchInPlace(Transformation transformation,
                                     List<Map<String, Object>> batch,
                                     variablestore varstore) {

        int rowsIn = batch == null ? 0 : batch.size();
        long start = TransformerMetrics.startNs();

        try {
            if (batch == null || batch.isEmpty()) {
                TransformerMetrics.log(log, "CUSTOM_COLUMN_GENERATOR", rowsIn, rowsIn, 0, start);
                return;
            }

            CustomColumnGeneratorSettings settings =
                    transformation.getSettingsAsType(CustomColumnGeneratorSettings.class);

            List<CustomColumnGeneratorSettings.Column> columns =
                    settings == null ? null : settings.getColumns();

            // ⚡ short-circuit: nothing configured
            if (columns == null || columns.isEmpty()) {
                TransformerMetrics.log(log, "CUSTOM_COLUMN_GENERATOR", rowsIn, rowsIn, 0, start);
                return;
            }

            // not removing any rows here
            for (Map<String, Object> row : batch) {
                for (CustomColumnGeneratorSettings.Column col : columns) {
                    if (col == null || col.getColumnName() == null) continue;

                    String key = PREFIX + col.getColumnName();

                    // your original behavior: only set if missing
                    if (!row.containsKey(key)) {
                        String raw = col.getValue();
                        String resolved = raw == null ? null : varstore.resolveVariables(raw);
                        row.put(key, resolved);
                    }
                }
            }

            TransformerMetrics.log(log, "CUSTOM_COLUMN_GENERATOR", rowsIn, batch.size(), 0, start);
        } catch (Exception ex) {
            log.error("CUSTOM_COLUMN_GENERATOR failed. rowsIn={}", rowsIn, ex);
            throw ex;
        }
    }
}
