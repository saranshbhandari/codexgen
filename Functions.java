package variable;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Base64;

final class Functions {

    static Object invoke(String n, List<Object> a) {
        switch (n) {

            case "TRIM": return s(a,0).trim();
            case "LTRIM": return s(a,0).replaceAll("^\\s+","");
            case "RTRIM": return s(a,0).replaceAll("\\s+$","");
            case "UPPER": return s(a,0).toUpperCase();
            case "LOWER": return s(a,0).toLowerCase();

            case "CAPITALCASE": {
                String s = s(a,0);
                return s.isEmpty() ? s :
                        Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
            }

            case "CAMELCASE": {
                String in = s(a,0);
                if (in.isBlank()) return "";
                String[] p = in.split("[_\\s-]+");
                StringBuilder sb = new StringBuilder(p[0].toLowerCase());
                for (int i=1;i<p.length;i++) {
                    if (p[i].isEmpty()) continue;
                    sb.append(Character.toUpperCase(p[i].charAt(0)))
                      .append(p[i].substring(1).toLowerCase());
                }
                return sb.toString();
            }

            case "SUBSTR": {
                String in = s(a,0);
                int start = i(a,1);
                int end;
                if (a.size() > 2) {
                    end = i(a,2);
                } else {
                    end = in.length();
                }
                // bounds safety
                start = Math.max(0, Math.min(start, in.length()));
                end = Math.max(0, Math.min(end, in.length()));
                if (end < start) return "";
                return in.substring(start, end);
            }

            case "REPLACE": return s(a,0).replace(s(a,1), s(a,2));
            case "LENGTH": return s(a,0).length();

            case "ADD": return d(a,0) + d(a,1);
            case "SUB": return d(a,0) - d(a,1);
            case "MUL": return d(a,0) * d(a,1);
            case "DIV": return d(a,0) / d(a,1);
            case "MAX": return Math.max(d(a,0), d(a,1));
            case "MIN": return Math.min(d(a,0), d(a,1));

            case "NVL": return a.get(0) != null ? a.get(0) : a.get(1);
            case "COALESCE": return a.stream().filter(Objects::nonNull).findFirst().orElse(null);
            case "DEFAULT_IF_BLANK": return s(a,0).isBlank() ? a.get(1) : s(a,0);

            case "NOW": return new Date();

            case "FORMAT_DATE": {
                Object dt = a.get(0);
                if (dt == null) return "";
                Date date = (dt instanceof Date) ? (Date) dt : new Date(Long.parseLong(String.valueOf(dt)));
                return new SimpleDateFormat(s(a,1)).format(date);
            }

            case "SIZE": return size(a.get(0));

            case "FIRST": {
                Object listObj = a.get(0);
                if (listObj == null) return null;
                if (listObj instanceof List) {
                    List<?> l = (List<?>) listObj;
                    return l.isEmpty() ? null : l.get(0);
                }
                if (listObj.getClass().isArray()) {
                    return java.lang.reflect.Array.getLength(listObj) == 0 ? null : java.lang.reflect.Array.get(listObj, 0);
                }
                if (listObj instanceof Iterable) {
                    Iterator<?> it = ((Iterable<?>) listObj).iterator();
                    return it.hasNext() ? it.next() : null;
                }
                return listObj;
            }

            case "LAST": {
                Object listObj = a.get(0);
                if (listObj == null) return null;
                if (listObj instanceof List) {
                    List<?> l = (List<?>) listObj;
                    return l.isEmpty() ? null : l.get(l.size() - 1);
                }
                if (listObj.getClass().isArray()) {
                    int len = java.lang.reflect.Array.getLength(listObj);
                    return len == 0 ? null : java.lang.reflect.Array.get(listObj, len - 1);
                }
                if (listObj instanceof Iterable) {
                    Object last = null;
                    for (Object item : (Iterable<?>) listObj) last = item;
                    return last;
                }
                return listObj;
            }

            case "JOIN": {
                Object listObj = a.get(0);
                String delim = s(a,1);
                if (listObj == null) return "";

                // arrays
                if (listObj.getClass().isArray()) {
                    int len = java.lang.reflect.Array.getLength(listObj);
                    StringBuilder sb = new StringBuilder();
                    for (int idx = 0; idx < len; idx++) {
                        if (idx > 0) sb.append(delim);
                        Object item = java.lang.reflect.Array.get(listObj, idx);
                        sb.append(item == null ? "" : String.valueOf(item));
                    }
                    return sb.toString();
                }

                // iterables (List, Set, etc.)
                if (listObj instanceof Iterable) {
                    StringBuilder sb = new StringBuilder();
                    boolean first = true;
                    for (Object item : (Iterable<?>) listObj) {
                        if (!first) sb.append(delim);
                        sb.append(item == null ? "" : String.valueOf(item));
                        first = false;
                    }
                    return sb.toString();
                }

                // single value fallback
                return String.valueOf(listObj);
            }

            case "BASE64_ENCODE":
                return Base64.getEncoder().encodeToString(s(a,0).getBytes(StandardCharsets.UTF_8));

            case "BASE64_DECODE":
                return new String(Base64.getDecoder().decode(s(a,0)), StandardCharsets.UTF_8);

            case "UUID": return UUID.randomUUID().toString();
        }

        throw new IllegalArgumentException("Unknown FXN: " + n);
    }

    static boolean truthy(Object o) {
        if (o == null) return false;
        if (o instanceof Boolean) return (Boolean) o;
        if (o instanceof Number) return ((Number) o).doubleValue() != 0;
        String s = o.toString().trim();
        if (s.isEmpty()) return false;
        return Boolean.parseBoolean(s);
    }

    private static String s(List<Object> a, int i) { return Objects.toString(a.get(i), ""); }
    private static int i(List<Object> a, int i) { return Integer.parseInt(s(a, i)); }
    private static double d(List<Object> a, int i) { return Double.parseDouble(s(a, i)); }

    private static int size(Object o) {
        if (o == null) return 0;
        if (o instanceof Collection) return ((Collection<?>) o).size();
        if (o instanceof Map) return ((Map<?, ?>) o).size();
        if (o.getClass().isArray()) return java.lang.reflect.Array.getLength(o);
        if (o instanceof Iterable) {
            int c = 0;
            for (Object ignored : (Iterable<?>) o) c++;
            return c;
        }
        return 1;
    }
}
