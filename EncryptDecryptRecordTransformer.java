package com.test.dataflowengine.processors.taskprocessorshelpers.transformers;

import com.test.dataflowengine.models.enums.EncryptDecryptAction;
import com.test.dataflowengine.models.enums.EncryptDecryptOnFailureAction;
import com.test.dataflowengine.models.tasksettings.subsettings.transformations.EncryptDecryptTransformationSettings;
import com.test.dataflowengine.models.tasksettings.subsettings.transformations.Transformation;
import com.test.dataflowengine.variablemanager.variablestore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Service
public class EncryptDecryptRecordTransformer implements RecordTransformer {

    private static final Logger log = LoggerFactory.getLogger(EncryptDecryptRecordTransformer.class);

    // You already have this in project:
    // @Autowired public CryptoUtil cryptoUtil;
    private final CryptoUtil cryptoUtil;

    public EncryptDecryptRecordTransformer(CryptoUtil cryptoUtil) {
        this.cryptoUtil = cryptoUtil;
    }

    @Override
    public void transformBatchInPlace(Transformation transformation,
                                     List<Map<String, Object>> batch,
                                     variablestore varstore) {

        int rowsIn = batch == null ? 0 : batch.size();
        long start = TransformerMetrics.startNs();

        try {
            EncryptDecryptTransformationSettings s =
                    transformation.getSettingsAsType(EncryptDecryptTransformationSettings.class);

            EncryptDecryptOnFailureAction onFailure = s.getOnFailure();
            if (onFailure == null) onFailure = EncryptDecryptOnFailureAction.ABORT;

            if (s.getAction() == EncryptDecryptAction.ENCRYPT) {
                encryptBatch(s.getColumnsToEncrypt(), batch, onFailure);
            } else if (s.getAction() == EncryptDecryptAction.DECRYPT) {
                decryptBatch(s.getColumnsToEncrypt(), batch, onFailure);
            }

            int rowsOut = batch == null ? 0 : batch.size();
            int removed = Math.max(0, rowsIn - rowsOut);
            TransformerMetrics.log(log, "ENCRYPT_DECRYPT_TRANSFORMATION", rowsIn, rowsOut, removed, start);

        } catch (Exception ex) {
            log.error("ENCRYPT_DECRYPT_TRANSFORMATION failed. rowsIn={}", rowsIn, ex);
            throw ex;
        }
    }

    private void encryptBatch(List<String> columns,
                             List<Map<String, Object>> batch,
                             EncryptDecryptOnFailureAction onFailure) {

        if (batch == null) return;

        Iterator<Map<String, Object>> it = batch.iterator();
        while (it.hasNext()) {
            Map<String, Object> row = it.next();

            Map<String, Object> snapshot = Map.copyOf(row); // for COPY_AS_IT_IS

            try {
                if (columns != null) {
                    for (String col : columns) {
                        if (col == null) continue;
                        Object val = row.get(col);
                        if (val != null) {
                            row.put(col, cryptoUtil.encryptString(val.toString()));
                        }
                    }
                }
            } catch (Exception ex) {
                handleFailure(onFailure, it, row, snapshot, ex, "encrypt");
            }
        }
    }

    private void decryptBatch(List<String> columns,
                             List<Map<String, Object>> batch,
                             EncryptDecryptOnFailureAction onFailure) {

        if (batch == null) return;

        Iterator<Map<String, Object>> it = batch.iterator();
        while (it.hasNext()) {
            Map<String, Object> row = it.next();

            Map<String, Object> snapshot = Map.copyOf(row);

            try {
                if (columns != null) {
                    for (String col : columns) {
                        if (col == null) continue;
                        Object val = row.get(col);
                        if (val != null) {
                            row.put(col, cryptoUtil.decryptString(val.toString()));
                        }
                    }
                }
            } catch (Exception ex) {
                handleFailure(onFailure, it, row, snapshot, ex, "decrypt");
            }
        }
    }

    private void handleFailure(EncryptDecryptOnFailureAction onFailure,
                               Iterator<Map<String, Object>> it,
                               Map<String, Object> row,
                               Map<String, Object> snapshot,
                               Exception ex,
                               String mode) {

        if (onFailure == EncryptDecryptOnFailureAction.SKIP_FAILED_ROW) {
            it.remove();
            log.warn("EncryptDecrypt {}: removed failed row due to SKIP_FAILED_ROW. row={}", mode, row, ex);
            return;
        }

        if (onFailure == EncryptDecryptOnFailureAction.COPY_AS_IT_IS) {
            row.clear();
            row.putAll(snapshot);
            log.warn("EncryptDecrypt {}: kept original row due to COPY_AS_IT_IS. row={}", mode, row, ex);
            return;
        }

        // ABORT
        throw new RuntimeException("Error while " + mode + " row=" + row, ex);
    }

    // Placeholder for your existing util
    public interface CryptoUtil {
        String encryptString(String s);
        String decryptString(String s);
    }
}
