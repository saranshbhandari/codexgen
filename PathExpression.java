package variable;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

final class PathExpression implements Expression {

    private final String base;
    private final List<PathToken> path;

    PathExpression(String base, List<PathToken> path) {
        this.base = base;
        this.path = path;
    }

    public Object eval(VariableStore store) {
        Object cur = store.resolveBase(base);

        for (PathToken t : path) {
            if (cur == null) return null;
            cur = resolveNext(cur, t.value);
        }
        return cur;
    }

    private Object resolveNext(Object cur, String key) {
        try {
            if (cur instanceof Map)
                return ((Map<?, ?>) cur).get(key);

            if (cur instanceof List)
                return ((List<?>) cur).get(Integer.parseInt(key));

            if (cur.getClass().isArray())
                return Array.get(cur, Integer.parseInt(key));

            Method m = cur.getClass().getMethod(
                    "get" + Character.toUpperCase(key.charAt(0)) + key.substring(1));
            return m.invoke(cur);

        } catch (Exception e) {
            return null;
        }
    }
}
