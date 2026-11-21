package etl.mr;

/**
 * Contract 2: every q1/q2/q3/malformed output is PostgreSQL COPY text format.
 * Fields are separated by a tab; inside a field, backslash, tab, newline and carriage return
 * must be escaped. Build each output line with row(...) and write it as the key with a
 * NullWritable value (TextOutputFormat then writes exactly that line).
 */
public final class CopyText {
    private CopyText() {}

    public static String escape(String value) {
        if (value == null) return "";          // contract: missing parts are '', never NULL
        StringBuilder b = new StringBuilder(value.length() + 8);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\': b.append("\\\\"); break;
                case '\t': b.append("\\t"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                default: b.append(c);
            }
        }
        return b.toString();
    }

    /** One output line: fields escaped and joined with tabs. Numbers are written with toString(). */
    public static String row(Object... fields) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) b.append('\t');
            b.append(escape(fields[i] == null ? "" : fields[i].toString()));
        }
        return b.toString();
    }
}
