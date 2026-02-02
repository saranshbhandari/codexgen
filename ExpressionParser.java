package variable;

import java.util.*;

final class ExpressionParser {

    static Expression parse(String raw) {
        raw = raw.trim();

        if (raw.startsWith("FXN.")) {
            return parseFunction(raw.substring(4));
        }

        List<PathToken> tokens = tokenizePath(raw);
        if (tokens.size() < 2)
            throw new IllegalArgumentException("Invalid expression: " + raw);

        String base = "${" + tokens.get(0).value + "." + tokens.get(1).value + "}";
        List<PathToken> rest = tokens.subList(2, tokens.size());

        return new PathExpression(base, rest);
    }

    private static Expression parseFunction(String f) {
        int idx = f.indexOf('(');
        String name = f.substring(0, idx);
        String args = f.substring(idx + 1, f.lastIndexOf(')'));

        List<Expression> exprs = new ArrayList<>();
        int depth = 0;
        StringBuilder buf = new StringBuilder();

        for (char c : args.toCharArray()) {
            if (c == ',' && depth == 0) {
                exprs.add(parse(buf.toString()));
                buf.setLength(0);
            } else {
                if (c == '(') depth++;
                if (c == ')') depth--;
                buf.append(c);
            }
        }
        if (buf.length() > 0) exprs.add(parse(buf.toString()));

        return new FunctionExpression(name, exprs);
    }

    private static List<PathToken> tokenizePath(String raw) {
        List<PathToken> out = new ArrayList<>();
        StringBuilder buf = new StringBuilder();
        boolean inBracket = false;
        boolean nullSafe = false;

        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);

            if (!inBracket && c == '?' && i + 1 < raw.length() && raw.charAt(i + 1) == '.') {
                nullSafe = true;
                i++;
                continue;
            }

            if (!inBracket && c == '.') {
                out.add(new PathToken(buf.toString(), nullSafe));
                buf.setLength(0);
                nullSafe = false;
            } else if (c == '[') {
                out.add(new PathToken(buf.toString(), nullSafe));
                buf.setLength(0);
                nullSafe = false;
                inBracket = true;
            } else if (c == ']') {
                out.add(new PathToken(buf.toString(), false));
                buf.setLength(0);
                inBracket = false;
            } else {
                buf.append(c);
            }
        }

        if (buf.length() > 0)
            out.add(new PathToken(buf.toString(), nullSafe));

        return out;
    }
}
