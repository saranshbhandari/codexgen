package com.test.dataflowengine.processors.taskprocessorshelpers.transformers.filter;

import com.test.dataflowengine.variablemanager.variablestore;

import java.util.Map;

@FunctionalInterface
public interface RowPredicate {
    boolean test(Map<String, Object> row, variablestore varstore);
}
