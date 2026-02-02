package variable;

interface TemplatePart {
    String render(VariableStore store);
}

final class TextPart implements TemplatePart {
    private final String text;
    TextPart(String t) { this.text = t; }
    public String render(VariableStore store) { return text; }
}

final class ExprPart implements TemplatePart {
    private final Expression expr;
    ExprPart(Expression e) { this.expr = e; }

    public String render(VariableStore store) {
        Object v = store.resolveExpression(expr);
        return v == null ? "" : String.valueOf(v);
    }
}
