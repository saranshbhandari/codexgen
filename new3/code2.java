package com.yourpackage.task.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class CallStoredProcedureTaskProcessor implements TaskProcessor {

    private final StoredProcedureExecutorFactory storedProcedureExecutorFactory;
    private final DataSourceService dataSourceService;
    private final ObjectMapper objectMapper;

    @Override
    public String getTaskType() {
        return "CALL_STORED_PROCEDURE";
    }

    @Override
    public void process(Task task, VariableStore varstore) {
        long start = System.currentTimeMillis();
        ExecuteStoredProcedureTaskSettings settings = null;

        try {
            settings = objectMapper.convertValue(
                    task.getSettings(),
                    ExecuteStoredProcedureTaskSettings.class
            );

            if (settings == null) {
                throw new IllegalArgumentException("Stored procedure task settings are missing.");
            }

            resolveSettingsVariables(settings, varstore);

            DataSource ds = dataSourceService.getDataSource(settings.getDatasourceId());
            if (ds == null) {
                throw new IllegalArgumentException("No datasource found for id: " + settings.getDatasourceId());
            }

            StoredProcedureExecutor executor =
                    storedProcedureExecutorFactory.getExecutor(settings.getDatabaseType());

            StoredProcResult sp = executor.execute(ds, settings);

            String taskPrefix = "Task" + task.getId() + ".";

            Map<String, Object> resultSetMap = buildResultSetMap(sp);
            Map<String, Object> outVariables = safeMap(sp.getOutParams());

            // Main grouped variables
            varstore.addVariable("${" + taskPrefix + "ResultSet}", resultSetMap);
            varstore.addVariable("${" + taskPrefix + "OutVariables}", outVariables);
            varstore.addVariable("${" + taskPrefix + "UpdateCount}", sp.getUpdateCountSum());

            // Shortcuts + metadata
            addResultSetShortcuts(taskPrefix, resultSetMap, varstore);

            log.info(
                    "Stored procedure task completed. taskId={}, procedureName={}, resultSetKeys={}, outVariableKeys={}, updateCount={}, durationMs={}",
                    task.getId(),
                    settings.getProcedureName(),
                    resultSetMap.keySet(),
                    outVariables.keySet(),
                    sp.getUpdateCountSum(),
                    (System.currentTimeMillis() - start)
            );

        } catch (Exception ex) {
            log.error(
                    "Error while executing stored procedure task. taskId={}, procedureName={}, error={}",
                    task != null ? task.getId() : null,
                    settings != null ? settings.getProcedureName() : null,
                    ex.getMessage(),
                    ex
            );
            throw new RuntimeException("Failed to execute stored procedure task", ex);
        }
    }

    private void resolveSettingsVariables(ExecuteStoredProcedureTaskSettings settings, VariableStore varstore) {
        if (settings == null || settings.getParams() == null) {
            return;
        }

        for (ExecuteStoredProcedureTaskSettings.Parameter parameter : settings.getParams()) {
            if (parameter == null) {
                continue;
            }

            String type = parameter.getType() == null ? "IN" : parameter.getType().trim();
            if ("IN".equalsIgnoreCase(type) || "INOUT".equalsIgnoreCase(type)) {
                String currentValue = parameter.getValue();
                if (currentValue != null) {
                    parameter.setValue(varstore.resolveVariables(currentValue));
                }
            }
        }
    }

    /**
     * Builds one final ResultSet map containing:
     * 1. named result sets from cursor names (preferred)
     * 2. unnamed fallback result sets as ResultSet1, ResultSet2, ...
     */
    private Map<String, Object> buildResultSetMap(StoredProcResult sp) {
        Map<String, Object> finalMap = new LinkedHashMap<>();

        // 1. Named result sets from executor (cursor names, aliases, etc.)
        Map<String, Object> namedResultSets = safeMap(sp.getNamedResultSets());
        finalMap.putAll(namedResultSets);

        // 2. Add unnamed/fallback result sets if missing
        List<List<Map<String, Object>>> rawResultSets = sp.getResultSets();
        if (rawResultSets != null) {
            for (int i = 0; i < rawResultSets.size(); i++) {
                String fallbackKey = "ResultSet" + (i + 1);
                finalMap.putIfAbsent(fallbackKey, rawResultSets.get(i));
            }
        }

        return finalMap;
    }

    private void addResultSetShortcuts(String taskPrefix,
                                       Map<String, Object> resultSetMap,
                                       VariableStore varstore) {

        List<Map.Entry<String, Object>> datasetEntries = extractDatasetEntries(resultSetMap);

        // first resultset shortcut
        List<Map<String, Object>> firstResultSet = datasetEntries.size() >= 1
                ? castToRowList(datasetEntries.get(0).getValue())
                : Collections.emptyList();

        varstore.addVariable("${" + taskPrefix + "FirstResultSet}", firstResultSet);
        varstore.addVariable("${" + taskPrefix + "FirstResultSetCount}", firstResultSet.size());
        varstore.addVariable("${" + taskPrefix + "FirstResultSetColumns}", extractColumns(firstResultSet));
        varstore.addVariable("${" + taskPrefix + "FirstResultSetFirstRow}", firstResultSet.isEmpty() ? Collections.emptyMap() : firstResultSet.get(0));
        varstore.addVariable("${" + taskPrefix + "FirstResultSetLastRow}", firstResultSet.isEmpty() ? Collections.emptyMap() : firstResultSet.get(firstResultSet.size() - 1));

        // second resultset shortcut
        List<Map<String, Object>> secondResultSet = datasetEntries.size() >= 2
                ? castToRowList(datasetEntries.get(1).getValue())
                : Collections.emptyList();

        varstore.addVariable("${" + taskPrefix + "SecondResultSet}", secondResultSet);
        varstore.addVariable("${" + taskPrefix + "SecondResultSetCount}", secondResultSet.size());
        varstore.addVariable("${" + taskPrefix + "SecondResultSetColumns}", extractColumns(secondResultSet));
        varstore.addVariable("${" + taskPrefix + "SecondResultSetFirstRow}", secondResultSet.isEmpty() ? Collections.emptyMap() : secondResultSet.get(0));
        varstore.addVariable("${" + taskPrefix + "SecondResultSetLastRow}", secondResultSet.isEmpty() ? Collections.emptyMap() : secondResultSet.get(secondResultSet.size() - 1));

        // optional alias if you still want MainResultSet as first resultset
        varstore.addVariable("${" + taskPrefix + "MainResultSet}", firstResultSet);
    }

    /**
     * Keep only entries that look like List<Map<String,Object>> result sets.
     */
    private List<Map.Entry<String, Object>> extractDatasetEntries(Map<String, Object> resultSetMap) {
        List<Map.Entry<String, Object>> datasets = new ArrayList<>();
        if (resultSetMap == null || resultSetMap.isEmpty()) {
            return datasets;
        }

        for (Map.Entry<String, Object> entry : resultSetMap.entrySet()) {
            if (looksLikeRowList(entry.getValue())) {
                datasets.add(entry);
            }
        }

        return datasets;
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> castToRowList(Object value) {
        if (value == null) {
            return Collections.emptyList();
        }
        if (value instanceof List<?> list) {
            if (list.isEmpty()) {
                return Collections.emptyList();
            }
            Object first = list.get(0);
            if (first instanceof Map<?, ?>) {
                return (List<Map<String, Object>>) value;
            }
        }
        return Collections.emptyList();
    }

    private boolean looksLikeRowList(Object value) {
        if (!(value instanceof List<?> list)) {
            return false;
        }
        if (list.isEmpty()) {
            return true; // empty result set is still a valid dataset
        }
        return list.get(0) instanceof Map<?, ?>;
    }

    private List<String> extractColumns(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return Collections.emptyList();
        }
        Map<String, Object> firstRow = rows.get(0);
        if (firstRow == null || firstRow.isEmpty()) {
            return Collections.emptyList();
        }
        return new ArrayList<>(firstRow.keySet());
    }

    private Map<String, Object> safeMap(Map<String, Object> source) {
        return source == null ? new LinkedHashMap<>() : new LinkedHashMap<>(source);
    }
}
