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

        // ✅ Short-circuit IF(condition, trueExpr, falseExpr)
        if ("IF".equals(name)) {
            if (args.size() != 3) {
                throw new IllegalArgumentException("IF requires 3 arguments: IF(condition, trueValue, falseValue)");
            }
            Object cond = store.resolveExpression(args.get(0));
            return Functions.truthy(cond)
                    ? store.resolveExpression(args.get(1))
                    : store.resolveExpression(args.get(2));
        }

        // ✅ Short-circuit SWITCH(value, case1, result1, case2, result2, ..., default?)
        if ("SWITCH".equals(name)) {
            if (args.size() < 3) {
                throw new IllegalArgumentException(
                        "SWITCH requires: SWITCH(value, case1, result1, ..., default?)");
            }

            Object switchVal = store.resolveExpression(args.get(0));

            int i = 1;
            int remaining = args.size() - 1;

            // If remaining is odd -> last arg is default
            boolean hasDefault = (remaining % 2 == 1);
            int pairsEnd = args.size() - (hasDefault ? 1 : 0);

            while (i < pairsEnd) {
                Object caseVal = store.resolveExpression(args.get(i));
                if (Functions.areEqual(switchVal, caseVal)) {
                    return store.resolveExpression(args.get(i + 1));
                }
                i += 2;
            }

            if (hasDefault) {
                return store.resolveExpression(args.get(args.size() - 1));
            }

            return null;
        }

        // Normal functions evaluate all args
        java.util.ArrayList<Object> vals = new java.util.ArrayList<>(args.size());
        for (Expression e : args) {
            vals.add(store.resolveExpression(e));
        }
        return Functions.invoke(name, vals);
    }
}
