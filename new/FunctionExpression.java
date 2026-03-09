package variable;

import java.util.List;

final class FunctionExpression implements Expression {

    private final String name;
    private final List<Expression> args;

    FunctionExpression(String name, List<Expression> args) {
        this.name = name.toUpperCase();
        this.args = args;
    }

    @Override
    public Object eval(VariableStore store) {

        // ✅ IF(condition, trueExpr, falseExpr) short-circuit
        if ("IF".equals(name)) {
            if (args.size() != 3) {
                throw new IllegalArgumentException("IF requires 3 args: IF(condition, trueValue, falseValue)");
            }
            Object cond = store.resolveExpression(args.get(0));
            return Functions.truthy(cond)
                    ? store.resolveExpression(args.get(1))
                    : store.resolveExpression(args.get(2));
        }

        // ✅ AND(cond1, cond2, ...) short-circuit
        if ("AND".equals(name)) {
            if (args.isEmpty()) return true; // AND() => true (neutral element)
            for (Expression e : args) {
                Object v = store.resolveExpression(e);
                if (!Functions.truthy(v)) return false;
            }
            return true;
        }

        // ✅ IN(value, option1, option2, ...) short-circuit
        if ("IN".equals(name)) {
            if (args.size() < 2) {
                throw new IllegalArgumentException("IN requires at least 2 args: IN(value, option1, ...)");
            }
            Object val = store.resolveExpression(args.get(0));
            for (int i = 1; i < args.size(); i++) {
                Object option = store.resolveExpression(args.get(i));
                if (Functions.areEqual(val, option)) return true;
            }
            return false;
        }

        // ✅ NOT_IN(value, option1, option2, ...)
        if ("NOT_IN".equals(name)) {
            if (args.size() < 2) {
                throw new IllegalArgumentException("NOT_IN requires at least 2 args: NOT_IN(value, option1, ...)");
            }
        
            Object val = store.resolveExpression(args.get(0));
        
            for (int i = 1; i < args.size(); i++) {
                Object option = store.resolveExpression(args.get(i));
                if (Functions.areEqual(val, option)) {
                    return false; // short-circuit
                }
            }
            return true;
        }
        
        // ✅ CONTAINS(text, substring)
        if ("CONTAINS".equals(name)) {
            if (args.size() != 2) {
                throw new IllegalArgumentException("CONTAINS requires 2 args: CONTAINS(text, substring)");
            }
        
            Object textObj = store.resolveExpression(args.get(0));
            Object subObj  = store.resolveExpression(args.get(1));
        
            if (textObj == null || subObj == null) return false;
        
            String text = String.valueOf(textObj);
            String sub  = String.valueOf(subObj);
        
            return text.contains(sub);
        }
        
        // ✅ CONTAINS_IGNORE_CASE(text, substring)
        if ("CONTAINS_IGNORE_CASE".equals(name)) {
            if (args.size() != 2) {
                throw new IllegalArgumentException("CONTAINS_IGNORE_CASE requires 2 args");
            }
        
            Object textObj = store.resolveExpression(args.get(0));
            Object subObj  = store.resolveExpression(args.get(1));
        
            if (textObj == null || subObj == null) return false;
        
            String text = String.valueOf(textObj).toLowerCase();
            String sub  = String.valueOf(subObj).toLowerCase();
        
            return text.contains(sub);
        }

        // ✅ OR(cond1, cond2, ...) short-circuit
        if ("OR".equals(name)) {
            if (args.isEmpty()) return false; // OR() => false (neutral element)
            for (Expression e : args) {
                Object v = store.resolveExpression(e);
                if (Functions.truthy(v)) return true;
            }
            return false;
        }

        // ✅ NOT(cond) short-circuit
        if ("NOT".equals(name)) {
            if (args.size() != 1) {
                throw new IllegalArgumentException("NOT requires 1 arg: NOT(condition)");
            }
            Object v = store.resolveExpression(args.get(0));
            return !Functions.truthy(v);
        }

        // ✅ SWITCH(value, case1, result1, ..., default?) short-circuit
        if ("SWITCH".equals(name)) {
            if (args.size() < 3) {
                throw new IllegalArgumentException("SWITCH requires: SWITCH(value, case1, result1, ..., default?)");
            }

            Object switchVal = store.resolveExpression(args.get(0));

            int remaining = args.size() - 1;
            boolean hasDefault = (remaining % 2 == 1);
            int pairsEnd = args.size() - (hasDefault ? 1 : 0);

            for (int i = 1; i < pairsEnd; i += 2) {
                Object caseVal = store.resolveExpression(args.get(i));
                if (Functions.areEqual(switchVal, caseVal)) {
                    return store.resolveExpression(args.get(i + 1));
                }
            }

            return hasDefault ? store.resolveExpression(args.get(args.size() - 1)) : null;
        }

        // Normal functions evaluate all args
        java.util.ArrayList<Object> vals = new java.util.ArrayList<>(args.size());
        for (Expression e : args) {
            vals.add(store.resolveExpression(e));
        }
        return Functions.invoke(name, vals);
    }
}
