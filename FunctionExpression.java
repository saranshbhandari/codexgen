package variable;

import java.util.ArrayList;
import java.util.List;

final class FunctionExpression implements Expression {

    private final String name;
    private final List<Expression> args;

    FunctionExpression(String name, List<Expression> args) {
        this.name = name.toUpperCase();
        this.args = args;
    }

    public Object eval(VariableStore store) {

        if ("IF".equals(name)) {
            Object cond = store.resolveExpression(args.get(0));
            return Functions.truthy(cond)
                    ? store.resolveExpression(args.get(1))
                    : store.resolveExpression(args.get(2));
        }

        List<Object> vals = new ArrayList<>(args.size());
        for (Expression e : args)
            vals.add(store.resolveExpression(e));

        return Functions.invoke(name, vals);
    }
}
