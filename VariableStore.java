package variable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class VariableStore {

    private static final Logger log = LoggerFactory.getLogger(VariableStore.class);

    public enum MissingVariablePolicy {
        KEEP_AS_IS,
        REPLACE_WITH_EMPTY,
        THROW_ERROR
    }

    private static final int MAX_EXPR_DEPTH = 50;

    /* ========================= STATE ========================= */

    private final Map<String, Object> permanent = new ConcurrentHashMap<>();

    private final ThreadLocal<Deque<Map<String, Object>>> overlays =
            ThreadLocal.withInitial(ArrayDeque::new);

    private final Map<String, CompiledTemplate> templateCache =
            new ConcurrentHashMap<>();

    private final ThreadLocal<Integer> exprDepth =
            ThreadLocal.withInitial(() -> 0);

    private volatile MissingVariablePolicy missingVariablePolicy =
            MissingVariablePolicy.REPLACE_WITH_EMPTY;

    /* ========================= CONSTRUCTORS ========================= */

    public VariableStore() { }

    public VariableStore(MissingVariablePolicy policy) {
        this.missingVariablePolicy = Objects.requireNonNull(policy);
    }

    /* ========================= CONFIG ========================= */

    public void setMissingVariablePolicy(MissingVariablePolicy policy) {
        this.missingVariablePolicy = Objects.requireNonNull(policy);
        log.info("MissingVariablePolicy set to {}", policy);
    }

    public MissingVariablePolicy getMissingVariablePolicy() {
        return missingVariablePolicy;
    }

    /* ========================= VARIABLES ========================= */

    public void addVariable(String name, Object value) {
        if (value == null) return;
        String base = normalizeBase(name);
        permanent.put(base, value);
        log.debug("Added permanent variable {}", base);
    }

    public Scope withTempVariable(String name, Object value) {
        String base = normalizeBase(name);
        Map<String, Object> layer = new HashMap<>();
        layer.put(base, value);

        overlays.get().push(layer);
        log.debug("Pushed temp variable {}", base);

        return () -> {
            overlays.get().pop();
            if (overlays.get().isEmpty()) overlays.remove();
            log.debug("Popped temp variable {}", base);
        };
    }

    /* ========================= RESOLUTION ========================= */

    public String resolveVariables(String template) {
        if (template == null || template.isEmpty()) return template;

        CompiledTemplate ct = templateCache.computeIfAbsent(template, t -> {
            log.debug("Template cache MISS, compiling [{}]", t);
            return TemplateCompiler.compile(t);
        });

        return ct.evaluate(this);
    }

    /* ========================= INTERNAL ========================= */

    Object resolveExpression(Expression expr) {
        int depth = exprDepth.get();
        if (depth >= MAX_EXPR_DEPTH) {
            throw new IllegalStateException(
                    "Expression depth exceeded " + MAX_EXPR_DEPTH + " (possible recursion)"
            );
        }

        exprDepth.set(depth + 1);
        try {
            return expr.eval(this);
        } finally {
            exprDepth.set(depth);
        }
    }

    Object resolveBase(String baseVar) {
        Object v = fromOverlay(baseVar);
        if (v != null) {
            log.debug("Resolved {} from TEMP overlay", baseVar);
            return v;
        }

        v = permanent.get(baseVar);
        if (v != null) {
            log.debug("Resolved {} from PERMANENT store", baseVar);
        } else {
            log.debug("Variable {} not found", baseVar);
        }
        return v;
    }

    MissingVariablePolicy policy() {
        return missingVariablePolicy;
    }

    private Object fromOverlay(String base) {
        Deque<Map<String, Object>> stack = overlays.get();
        if (stack == null) return null;

        for (Map<String, Object> layer : stack) {
            if (layer.containsKey(base)) return layer.get(base);
        }
        return null;
    }

    private static String normalizeBase(String name) {
        String s = name.trim();
        if (!s.startsWith("${")) s = "${" + s + "}";
        int idx = s.indexOf('.', 2);
        return idx == -1 ? s : s.substring(0, s.indexOf('}', idx) + 1);
    }

    /* ========================= SCOPE ========================= */

    public interface Scope extends AutoCloseable {
        @Override void close();
    }
}
