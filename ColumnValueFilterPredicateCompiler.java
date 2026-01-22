package com.test.dataflowengine.processors.taskprocessorshelpers.transformers.filter;

import com.test.dataflowengine.models.enums.ConditionOperator;
import com.test.dataflowengine.models.enums.GroupOperator;
import com.test.dataflowengine.models.tasksettings.subsettings.transformations.ColumnValueFilterSettings;
import com.test.dataflowengine.variablemanager.variablestore;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class ColumnValueFilterPredicateCompiler {

    private ColumnValueFilterPredicateCompiler() {}

    public static RowPredicate compile(ColumnValueFilterSettings settings) {
        if (settings == null || settings.getGroups() == null || settings.getGroups().isEmpty()) {
            // No filters => keep all
            return (row, varstore) -> true;
        }

        final GroupOperator betweenGroups = settings.getBetweenGroupsOperator() == null
                ? GroupOperator.AND
                : settings.getBetweenGroupsOperator();

        final List<ColumnValueFilterSettings.Group> groups = settings.getGroups();

        return (row, varstore) -> {
            boolean acc = (betweenGroups == GroupOperator.AND); // neutral element

            for (ColumnValueFilterSettings.Group g : groups) {
                boolean groupResult = evalGroup(g, row, varstore);

                if (betweenGroups == GroupOperator.AND) {
                    acc = acc && groupResult;
                    if (!acc) return false; // short-circuit
                } else {
                    acc = acc || groupResult;
                    if (acc) return true; // short-circuit
                }
            }

            return acc;
        };
    }

    private static boolean evalGroup(ColumnValueFilterSettings.Group group,
                                     Map<String, Object> row,
                                     variablestore varstore) {
        if (group == null || group.getConditions() == null || group.getConditions().isEmpty()) {
            // empty group => treat as true (doesn't filter out)
            return true;
        }

        GroupOperator op = group.getGroupOperator() == null ? GroupOperator.AND : group.getGroupOperator();
        boolean acc = (op == GroupOperator.AND);

        for (ColumnValueFilterSettings.Condition c : group.getConditions()) {
            boolean cond = evalCondition(c, row, varstore);

            if (op == GroupOperator.AND) {
                acc = acc && cond;
                if (!acc) return false;
            } else {
                acc = acc || cond;
                if (acc) return true;
            }
        }

        return acc;
    }

    private static boolean evalCondition(ColumnValueFilterSettings.Condition condition,
                                         Map<String, Object> row,
                                         variablestore varstore) {
        if (condition == null || condition.getOperator() == null) return true;

        String column = condition.getColumnName();
        Object actual = (column == null) ? null : row.get(column);

        // value can have variables; resolve at runtime so compiled predicate is reusable
        String expectedRaw = condition.getValue();
        String expected = expectedRaw == null ? null : varstore.resolveVariables(expectedRaw);

        ConditionOperator op = condition.getOperator();

        return switch (op) {
            case isnull -> actual == null;
            case isnotnull -> actual != null;

            // IMPORTANT: null is NOT empty string (your requirement)
            case isemptystring -> isEmptyString(actual);
            case isnotemptystring -> isNotEmptyString(actual);

            case equals -> equalsValue(actual, expected);
            case notequal -> !equalsValue(actual, expected);

            case equalsIgnoreCase -> equalsIgnoreCase(actual, expected);
            case notequalIgnoreCase -> !equalsIgnoreCase(actual, expected);
        };
    }

    private static boolean isEmptyString(Object actual) {
        if (actual == null) return false;                 // null != ""
        if (!(actual instanceof String s)) return false;  // only string qualifies
        return s.isEmpty();
    }

    private static boolean isNotEmptyString(Object actual) {
        if (actual == null) return false;
        if (!(actual instanceof String s)) return false;
        return !s.isEmpty();
    }

    private static boolean equalsIgnoreCase(Object actual, String expected) {
        if (actual == null || expected == null) return false;
        String a = Objects.toString(actual, null);
        return a != null && a.equalsIgnoreCase(expected);
    }

    private static boolean equalsValue(Object actual, String expected) {
        if (actual == null || expected == null) return false;

        // numeric-safe compare (optional but practical)
        if (actual instanceof Number) {
            BigDecimal a = toBigDecimal((Number) actual);
            BigDecimal e = tryParseBigDecimal(expected);
            if (a != null && e != null) {
                return a.compareTo(e) == 0;
            }
        }

        // fallback string compare
        return Objects.toString(actual, "").equals(expected);
    }

    private static BigDecimal toBigDecimal(Number n) {
        try {
            return new BigDecimal(n.toString());
        } catch (Exception ex) {
            return null;
        }
    }

    private static BigDecimal tryParseBigDecimal(String s) {
        try {
            return new BigDecimal(s);
        } catch (Exception ex) {
            return null;
        }
    }
}
