package variable;

import java.util.List;

final class CompiledTemplate {

    private final List<TemplatePart> parts;

    CompiledTemplate(List<TemplatePart> parts) {
        this.parts = parts;
    }

    String evaluate(VariableStore store) {
        StringBuilder sb = new StringBuilder();
        for (TemplatePart p : parts) {
            sb.append(p.render(store));
        }
        return sb.toString();
    }
}
