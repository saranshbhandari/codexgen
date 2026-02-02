package variable;

final class PathToken {
    final String value;
    final boolean nullSafe;

    PathToken(String value, boolean nullSafe) {
        this.value = value;
        this.nullSafe = nullSafe;
    }
}
