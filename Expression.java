package variable;

interface Expression {
    Object eval(VariableStore store);
}
