// Add variable using scope + key -> stored as "${scope.key}"
public void addVariable(String scope, String key, Object value) {
    if (value == null) return;
    String baseVar = "${" + Objects.requireNonNull(scope).trim() + "." + Objects.requireNonNull(key).trim() + "}";
    addVariable(baseVar, value);
}

// Add a batch of variables. Keys can be "Scope.Key" or "${Scope.Key}"
public void addVariables(Map<String, Object> variables) {
    if (variables == null || variables.isEmpty()) return;
    for (Map.Entry<String, Object> e : variables.entrySet()) {
        addVariable(e.getKey(), e.getValue());
    }
}

// Remove by either "Scope.Key" or "${Scope.Key}"
public void removeVariable(String name) {
    if (name == null || name.trim().isEmpty()) return;
    permanent.remove(normalizeBase(name));
}

// Clear everything permanent + cache (temporary overlays are per-thread)
public void clear() {
    permanent.clear();
    templateCache.clear();
    log.info("VariableStore cleared (permanent vars + template cache)");
}
