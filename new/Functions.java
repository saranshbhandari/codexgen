package variable;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Base64;

final class Functions {

    static Object invoke(String n, List<Object> a) {
        switch (n) {

            // ===== String =====
            case "TRIM": return s(a,0).trim();
            case "LTRIM": return s(a,0).replaceAll("^\\s+","");
            case "RTRIM": return s(a,0).replaceAll("\\s+$","");
            case "UPPER": return s(a,0).toUpperCase();
            case "LOWER": return s(a,0).toLowerCase();

            case "CAPITALCASE": {
                String x = s(a,0);
                return x.isEmpty() ? x : Character.toUpperCase(x.charAt(0)) + x.substring(1).toLowerCase();
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
                int end = (a.size() > 2) ? i(a,2) : in.length();
                start = Math.max(0, Math.min(start, in.length()));
                end = Math.max(0, Math.min(end, in.length()));
                if (end < start) return "";
                return in.substring(start, end);
            }

            case "REPLACE": return s(a,0).replace(s(a,1), s(a,2));
            case "LENGTH": return s(a,0).length();

            // ===== Numeric basic =====
            case "ADD": return d(a,0)+d(a,1);
            case "SUB": return d(a,0)-d(a,1);
            case "MUL": return d(a,0)*d(a,1);
            case "DIV": return d(a,0)/d(a,1);
            case "MAX": return Math.max(d(a,0),d(a,1));
            case "MIN": return Math.min(d(a,0),d(a,1));

            // ===== Numeric enhanced =====
            case "ABS": return Math.abs(d(a,0));
            case "CEIL": return Math.ceil(d(a,0));
            case "FLOOR": return Math.floor(d(a,0));
            case "POW": return Math.pow(d(a,0), d(a,1));
            case "MOD": return d(a,0) % d(a,1);

            // ROUND(num, precision)
            case "ROUND": {
                double num = d(a,0);
                int precision = (a.size() > 1) ? (int) Math.round(d(a,1)) : 0;
                double factor = Math.pow(10, precision);
                return Math.round(num * factor) / factor;
            }

            // ===== Conditional helpers (predicates) =====
            case "ISEQUAL": return areEqual(a0(a), a1(a));
            case "NOTEQUAL": return !areEqual(a0(a), a1(a));
            case "ISNULL": return a0(a) == null;
            case "ISNOTNULL": return a0(a) != null;
            case "ISBLANK": return s(a,0).isBlank();
            case "ISNOTBLANK": return !s(a,0).isBlank();
            case "ISTRUE": return truthy(a0(a));
            case "ISFALSE": return !truthy(a0(a));

            case "ISGREATERTHAN": return d(a,0) > d(a,1);
            case "ISLESSTHAN": return d(a,0) < d(a,1);
            case "ISGREATEROREQUAL": return d(a,0) >= d(a,1);
            case "ISLESSOREQUAL": return d(a,0) <= d(a,1);

            // ===== Null/default =====
            case "NVL": return a0(a)!=null ? a0(a) : a1(a);
            case "COALESCE": return a.stream().filter(Objects::nonNull).findFirst().orElse(null);
            case "DEFAULT_IF_BLANK": return s(a,0).isBlank() ? (a.size()>1?a.get(1):"") : s(a,0);

            // ===== Date =====
            case "NOW": return new Date();
            case "FORMAT_DATE": {
                Object dt = a0(a);
                if (dt == null) return "";
                Date date = (dt instanceof Date) ? (Date) dt : new Date(Long.parseLong(String.valueOf(dt)));
                return new SimpleDateFormat(s(a,1)).format(date);
            }

            // ===== Collections =====
            case "SIZE": return size(a0(a));
            case "FIRST": return first(a0(a));
            case "LAST": return last(a0(a));
            case "JOIN": return join(a0(a), s(a,1));

            // ===== Utils =====
            case "BASE64_ENCODE":
                return Base64.getEncoder().encodeToString(s(a,0).getBytes(StandardCharsets.UTF_8));
            case "BASE64_DECODE":
                return new String(Base64.getDecoder().decode(s(a,0)), StandardCharsets.UTF_8);
            case "UUID": return UUID.randomUUID().toString();
        }
        throw new IllegalArgumentException("Unknown FXN: " + n);
    }

    // ===== Public helpers used by FunctionExpression =====

    static boolean truthy(Object o) {
        if (o == null) return false;
        if (o instanceof Boolean) return (Boolean) o;
        if (o instanceof Number) return ((Number) o).doubleValue() != 0;
        String s = o.toString().trim();
        if (s.isEmpty()) return false;
        return Boolean.parseBoolean(s);
    }

    static boolean areEqual(Object a, Object b) {
        if (a == b) return true;
        if (a == null || b == null) return false;

        // numeric compare if both numeric-like
        if (isNumericLike(a) && isNumericLike(b)) {
            return Double.compare(toDouble(a), toDouble(b)) == 0;
        }

        return Objects.equals(String.valueOf(a), String.valueOf(b));
    }

    // ===== Internal helpers =====

    private static Object a0(List<Object> a){ return a.size()>0?a.get(0):null; }
    private static Object a1(List<Object> a){ return a.size()>1?a.get(1):null; }

    private static String s(List<Object>a,int i){ return Objects.toString(a.size()>i?a.get(i):"", ""); }
    private static int i(List<Object>a,int i){ return Integer.parseInt(s(a,i)); }
    private static double d(List<Object>a,int i){ return Double.parseDouble(s(a,i)); }

    private static boolean isNumericLike(Object o) {
        if (o instanceof Number) return true;
        if (o == null) return false;
        String s = o.toString().trim();
        if (s.isEmpty()) return false;
        try { Double.parseDouble(s); return true; }
        catch (Exception e) { return false; }
    }

    private static double toDouble(Object o) {
        if (o instanceof Number) return ((Number) o).doubleValue();
        return Double.parseDouble(String.valueOf(o).trim());
    }

    private static int size(Object o) {
        if (o == null) return 0;
        if (o instanceof Collection) return ((Collection<?>) o).size();
        if (o instanceof Map) return ((Map<?, ?>) o).size();
        if (o.getClass().isArray()) return java.lang.reflect.Array.getLength(o);
        if (o instanceof Iterable) {
            int c = 0; for (Object ignored : (Iterable<?>) o) c++; return c;
        }
        return 1;
    }

    private static Object first(Object o) {
        if (o == null) return null;
        if (o instanceof List) {
            List<?> l = (List<?>) o; return l.isEmpty()?null:l.get(0);
        }
        if (o.getClass().isArray()) {
            return java.lang.reflect.Array.getLength(o)==0?null:java.lang.reflect.Array.get(o,0);
        }
        if (o instanceof Iterable) {
            Iterator<?> it = ((Iterable<?>) o).iterator();
            return it.hasNext()?it.next():null;
        }
        return o;
    }

    private static Object last(Object o) {
        if (o == null) return null;
        if (o instanceof List) {
            List<?> l = (List<?>) o; return l.isEmpty()?null:l.get(l.size()-1);
        }
        if (o.getClass().isArray()) {
            int len = java.lang.reflect.Array.getLength(o);
            return len==0?null:java.lang.reflect.Array.get(o,len-1);
        }
        if (o instanceof Iterable) {
            Object last = null; for (Object item : (Iterable<?>) o) last = item; return last;
        }
        return o;
    }

    private static String join(Object listObj, String delim) {
        if (listObj == null) return "";

        if (listObj.getClass().isArray()) {
            int len = java.lang.reflect.Array.getLength(listObj);
            StringBuilder sb = new StringBuilder();
            for (int idx=0; idx<len; idx++) {
                if (idx>0) sb.append(delim);
                Object item = java.lang.reflect.Array.get(listObj, idx);
                sb.append(item==null?"":String.valueOf(item));
            }
            return sb.toString();
        }

        if (listObj instanceof Iterable) {
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Object item : (Iterable<?>) listObj) {
                if (!first) sb.append(delim);
                sb.append(item==null?"":String.valueOf(item));
                first = false;
            }
            return sb.toString();
        }

        return String.valueOf(listObj);
    }
}
