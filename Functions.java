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
                return s.isEmpty()?s:
                        Character.toUpperCase(s.charAt(0))+s.substring(1).toLowerCase();
            }

            case "CAMELCASE": {
                String[] p = s(a,0).split("[_\\s-]+");
                StringBuilder sb = new StringBuilder(p[0].toLowerCase());
                for (int i=1;i<p.length;i++)
                    sb.append(Character.toUpperCase(p[i].charAt(0)))
                      .append(p[i].substring(1).toLowerCase());
                return sb.toString();
            }

            case "SUBSTR": return s(a,0)
                    .substring(i(a,1), a.size()>2?i(a,2):s(a,0).length());

            case "REPLACE": return s(a,0).replace(s(a,1), s(a,2));
            case "LENGTH": return s(a,0).length();

            case "ADD": return d(a,0)+d(a,1);
            case "SUB": return d(a,0)-d(a,1);
            case "MUL": return d(a,0)*d(a,1);
            case "DIV": return d(a,0)/d(a,1);
            case "MAX": return Math.max(d(a,0),d(a,1));
            case "MIN": return Math.min(d(a,0),d(a,1));

            case "NVL": return a.get(0)!=null?a.get(0):a.get(1);
            case "COALESCE": return a.stream().filter(Objects::nonNull).findFirst().orElse(null);
            case "DEFAULT_IF_BLANK": return s(a,0).isBlank()?a.get(1):s(a,0);

            case "NOW": return new Date();
            case "FORMAT_DATE":
                return new SimpleDateFormat(s(a,1)).format((Date)a.get(0));

            case "SIZE": return size(a.get(0));
            case "FIRST": return ((List<?>)a.get(0)).get(0);
            case "LAST": {
                List<?> l=(List<?>)a.get(0);
                return l.get(l.size()-1);
            }
            case "JOIN": return String.join(s(a,1),(Collection<?>)a.get(0));

            case "BASE64_ENCODE":
                return Base64.getEncoder().encodeToString(s(a,0).getBytes(StandardCharsets.UTF_8));
            case "BASE64_DECODE":
                return new String(Base64.getDecoder().decode(s(a,0)),StandardCharsets.UTF_8);

            case "UUID": return UUID.randomUUID().toString();
        }
        throw new IllegalArgumentException("Unknown FXN: "+n);
    }

    static boolean truthy(Object o){
        if(o==null)return false;
        if(o instanceof Boolean)return (Boolean)o;
        if(o instanceof Number)return ((Number)o).doubleValue()!=0;
        return Boolean.parseBoolean(o.toString());
    }

    private static String s(List<Object>a,int i){return Objects.toString(a.get(i),"");}
    private static int i(List<Object>a,int i){return Integer.parseInt(s(a,i));}
    private static double d(List<Object>a,int i){return Double.parseDouble(s(a,i));}

    private static int size(Object o){
        if(o instanceof Collection)return ((Collection<?>)o).size();
        if(o instanceof Map)return ((Map<?,?>)o).size();
        return 0;
    }
}
