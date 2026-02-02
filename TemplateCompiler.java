package variable;

import java.util.ArrayList;
import java.util.List;

final class TemplateCompiler {

    static CompiledTemplate compile(String template) {
        List<TemplatePart> parts = new ArrayList<>();
        int i = 0;

        while (i < template.length()) {
            int start = template.indexOf("${", i);
            if (start < 0) {
                parts.add(new TextPart(template.substring(i)));
                break;
            }

            if (start > i) {
                parts.add(new TextPart(template.substring(i, start)));
            }

            int end = findClosing(template, start + 2);
            String expr = template.substring(start + 2, end);
            parts.add(new ExprPart(ExpressionParser.parse(expr)));

            i = end + 1;
        }
        return new CompiledTemplate(parts);
    }

    private static int findClosing(String s, int i) {
        int depth = 1;
        while (i < s.length()) {
            if (s.startsWith("${", i)) depth++;
            else if (s.charAt(i) == '}') {
                depth--;
                if (depth == 0) return i;
            }
            i++;
        }
        throw new IllegalArgumentException("Unclosed ${ in template");
    }
}
