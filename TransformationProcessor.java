package com.test.dataflowengine.processors.taskprocessorshelpers.transformers;

import com.test.dataflowengine.models.enums.TransformationType;
import com.test.dataflowengine.models.tasksettings.subsettings.transformations.Transformation;
import com.test.dataflowengine.variablemanager.variablestore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Service
public class TransformationsProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransformationsProcessor.class);

    private final Map<TransformationType, RecordTransformer> registry = new EnumMap<>(TransformationType.class);

    public TransformationsProcessor(ColumnValueFilterTransformer columnValueFilterTransformer,
                                    CustomValueGenerationTransformer customValueGenerationTransformer,
                                    EncryptDecryptRecordTransformer encryptDecryptRecordTransformer) {

        registry.put(TransformationType.COLUMN_VALUE_FILTER, columnValueFilterTransformer);
        registry.put(TransformationType.CUSTOM_COLUMN_GENERATOR, customValueGenerationTransformer);
        registry.put(TransformationType.ENCRYPT_DECRYPT_TRANSFORMATION, encryptDecryptRecordTransformer);
    }

    /**
     * If you want to reuse the SAME transformation list for MANY batches:
     *   CompiledTransformationPipeline pipeline = compile(transformations);
     *   pipeline.execute(batch1, varstore);
     *   pipeline.execute(batch2, varstore);
     */
    public CompiledTransformationPipeline compile(List<Transformation> transformations) {
        return new CompiledTransformationPipeline(transformations, registry);
    }

    /**
     * One-shot execution (still uses the same compiled approach internally).
     */
    public void processTransformations(List<Transformation> transformations,
                                       List<Map<String, Object>> batch,
                                       variablestore varstore) {

        compile(transformations).execute(batch, varstore);
    }
}
