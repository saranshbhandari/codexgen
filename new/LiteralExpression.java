package variable;

final class LiteralExpression implements Expression {

    private final Object value;

    LiteralExpression(Object value) {
        this.value = value;
    }

    @Override
    public Object eval(VariableStore store) {
        return value;
    }
}
