package variable;

import java.util.*;

final class ExpressionParser {

    static Expression parse(String raw) {
        if (raw == null) return new LiteralExpression(null);

        String s = raw.trim();

        // Allow callers to pass ${...} too
        if (s.startsWith("${") && s.endsWith("}")) {
            s = s.substring(2, s.length() - 1).trim();
        }

        // Literals
        Expression lit = tryParseLiteral(s);
        if (lit != null) return lit;

        // Functions
        if (s.startsWith("FXN.")) {
            return parseFunction(s.substring(4));
        }

        // Path
        List<PathToken> tokens = tokenizePath(s);
        if (tokens.size() < 2)
            throw new IllegalArgumentException("Invalid expression: " + raw);

        String base = "${" + tokens.get(0).value + "." + tokens.get(1).value + "}";
        List<PathToken> rest = tokens.subList(2, tokens.size());

        return new PathExpression(base, rest);
    }

    private static Expression tryParseLiteral(String s) {
        if (s.isEmpty()) return new LiteralExpression("");

        // null literal
        if ("null".equalsIgnoreCase(s)) return new LiteralExpression(null);

        // boolean literals
        if ("true".equalsIgnoreCase(s)) return new LiteralExpression(Boolean.TRUE);
        if ("false".equalsIgnoreCase(s)) return new LiteralExpression(Boolean.FALSE);

        // quoted string "..." or '...'
        if ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("'") && s.endsWith("'"))) {
            String inner = s.substring(1, s.length() - 1);
            inner = inner.replace("\\\"", "\"")
                         .replace("\\'", "'")
                         .replace("\\n", "\n")
                         .replace("\\t", "\t")
                         .replace("\\\\", "\\");
            return new LiteralExpression(inner);
        }

        // number literal (int/decimal)
        if (looksLikeNumber(s)) {
            try {
                // Keep as Double for numeric functions
                return new LiteralExpression(Double.parseDouble(s));
            } catch (NumberFormatException ignored) {
                // fall through; treat as path
            }
        }

        return null;
    }

    private static boolean looksLikeNumber(String s) {
        // simple numeric check: -12, 12.34
        int i = 0;
        if (s.startsWith("-")) i++;
        boolean dot = false;
        boolean digit = false;
        for (; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isDigit(c)) digit = true;
            else if (c == '.' && !dot) dot = true;
            else return false;
        }
        return digit;
    }

    private static Expression parseFunction(String f) {
        int idx = f.indexOf('(');
        if (idx < 0 || !f.endsWith(")"))
            throw new IllegalArgumentException("Invalid function call: FXN." + f);

        String name = f.substring(0, idx).trim();
        String inside = f.substring(idx + 1, f.length() - 1);

        List<String> argsRaw = splitArgs(inside);
        List<Expression> args = new ArrayList<>(argsRaw.size());
        for (String a : argsRaw) {
            String t = a.trim();
            if (!t.isEmpty()) args.add(parse(t));
            else args.add(new LiteralExpression("")); // allow empty arg
        }

        return new FunctionExpression(name, args);
    }

    /**
     * Splits function args safely:
     * - supports nested (...) and nested ${...}
     * - supports quoted strings with commas inside
     */
    private static List<String> splitArgs(String s) {
        List<String> out = new ArrayList<>();
        if (s == null || s.isBlank()) return out;

        StringBuilder buf = new StringBuilder();
        int parenDepth = 0;
        int varDepth = 0;
        boolean inQuotes = false;
        char quoteChar = 0;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            // Handle quotes
            if (inQuotes) {
                buf.append(c);
                if (c == quoteChar && (i == 0 || s.charAt(i - 1) != '\\')) {
                    inQuotes = false;
                    quoteChar = 0;
                }
                continue;
            } else if (c == '"' || c == '\'') {
                inQuotes = true;
                quoteChar = c;
                buf.append(c);
                continue;
            }

            // Track nested ${ ... }
            if (c == '$' && i + 1 < s.length() && s.charAt(i + 1) == '{') {
                varDepth++;
                buf.append(c);
                continue;
            }
            if (c == '}' && varDepth > 0) {
                varDepth--;
                buf.append(c);
                continue;
            }

            // Track parentheses nesting
            if (c == '(') {
                parenDepth++;
                buf.append(c);
                continue;
            }
            if (c == ')' && parenDepth > 0) {
                parenDepth--;
                buf.append(c);
                continue;
            }

            // Split on commas only at top-level
            if (c == ',' && parenDepth == 0 && varDepth == 0) {
                out.add(buf.toString());
                buf.setLength(0);
                continue;
            }

            buf.append(c);
        }

        out.add(buf.toString());
        return out;
    }

    // Your existing tokenizer, unchanged (dot + bracket + null-safe)
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
