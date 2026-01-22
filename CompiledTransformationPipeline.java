package com.test.dataflowengine.processors.taskprocessorshelpers.transformers;

import com.test.dataflowengine.models.enums.TransformationType;
import com.test.dataflowengine.models.tasksettings.subsettings.transformations.Transformation;
import com.test.dataflowengine.variablemanager.variablestore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class CompiledTransformationPipeline {

    private static final Logger log = LoggerFactory.getLogger(CompiledTransformationPipeline.class);

    private final List<CompiledStep> steps;

    CompiledTransformationPipeline(List<Transformation> transformations,
                                   Map<TransformationType, RecordTransformer> registry) {

        this.steps = new ArrayList<>();

        if (transformations == null || transformations.isEmpty()) {
            return;
        }

        for (Transformation t : transformations) {
            if (t == null || t.getTransformationtype() == null) continue;

            RecordTransformer transformer = registry.get(t.getTransformationtype());
            if (transformer == null) {
                log.warn("No transformer registered for transformationType={}", t.getTransformationtype());
                continue;
            }

            // compile step = bind transformer + transformation object
            steps.add(new CompiledStep(transformer, t));
        }
    }

    public void execute(List<Map<String, Object>> batch, variablestore varstore) {
        if (steps.isEmpty()) return;
        if (batch == null || batch.isEmpty()) return;

        for (CompiledStep s : steps) {
            s.apply(batch, varstore);

            // if a filter removed all rows, short-circuit the pipeline
            if (batch.isEmpty()) {
                return;
            }
        }
    }

    private static final class CompiledStep {
        private final RecordTransformer transformer;
        private final Transformation transformation;

        private CompiledStep(RecordTransformer transformer, Transformation transformation) {
            this.transformer = transformer;
            this.transformation = transformation;
        }

        private void apply(List<Map<String, Object>> batch, variablestore varstore) {
            transformer.transformBatchInPlace(transformation, batch, varstore);
        }
    }
}
