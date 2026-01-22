package com.test.dataflowengine.processors.taskprocessorshelpers.transformers;

import com.test.dataflowengine.models.tasksettings.subsettings.transformations.Transformation;
import com.test.dataflowengine.variablemanager.variablestore;

import java.util.List;
import java.util.Map;

public interface RecordTransformer {
    void transformBatchInPlace(
            Transformation transformation,
            List<Map<String, Object>> batch,
            variablestore varstore
    );
}
