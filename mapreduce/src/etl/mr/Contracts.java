package etl.mr;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The shared rules from contracts/, loaded at run time (never retyped).
 *
 * The folder is mounted at /contracts in the hadoop container. In Hadoop local mode every
 * mapper and reducer runs in the same container, so they can all read it directly.
 *
 * Use matcher(line).matches(): the contract regexes are anchored with ^ and $.
 */
public final class Contracts {
    public static final String DEFAULT_DIR = "/contracts";

    public final Pattern envelope;          // 13 groups: host ident user day month year hour min sec tz request status bytes
    public final Pattern request;           // method (1), resource_path (2), protocol_version (3, may be null)
    public final Pattern requestFallback;   // resource_path (1) when request does not match
    public final Map<String, String> months; // "Jul" -> "07"; any other month name = malformed

    private static Contracts instance;

    /** One copy per JVM. dir is usually DEFAULT_DIR; pass conf.get("etl.contracts.dir", DEFAULT_DIR). */
    public static synchronized Contracts load(String dir) throws IOException {
        if (instance == null) {
            instance = new Contracts(Paths.get(dir));
        }
        return instance;
    }

    private Contracts(Path dir) throws IOException {
        envelope = Pattern.compile(read(dir.resolve("envelope.regex")));
        request = Pattern.compile(read(dir.resolve("request.regex")));
        requestFallback = Pattern.compile(read(dir.resolve("request_fallback.regex")));
        months = new HashMap<>();
        List<String> rows = Files.readAllLines(dir.resolve("months.tsv"), StandardCharsets.UTF_8);
        for (String row : rows.subList(1, rows.size())) {          // first row is the header
            if (row.isEmpty()) continue;
            String[] f = row.split("\t");
            months.put(f[0], f[1]);
        }
    }

    private static String read(Path p) throws IOException {
        String s = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
        while (s.endsWith("\n") || s.endsWith("\r")) s = s.substring(0, s.length() - 1);
        return s;
    }
}
