package etl.pig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * Pig UDF: the UTF-8 bytes of a chararray as lowercase hex.
 *
 * Contract 2 ranks Q2 ties by resource_path in raw UTF-8 byte order (PostgreSQL's C collation,
 * Hadoop Text, MongoDB's simple collation). Pig compares chararrays as Java Strings, i.e. by
 * UTF-16 code units, which puts a character above U+FFFF (a surrogate pair, D800-DFFF) before
 * U+E000-U+FFFF; byte order puts it after. Two hex digits per byte, with '0'-'9' before 'a'-'f',
 * so ordering these keys as strings orders the original values by their bytes.
 *
 * Used in the Pig engine's Q2 script:  REGISTER 'etl-pig.jar'; DEFINE Utf8Key etl.pig.Utf8Key();
 */
public class Utf8Key extends EvalFunc<String> {
    private static final char[] HEX = "0123456789abcdef".toCharArray();

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) return null;
        byte[] bytes = input.get(0).toString().getBytes(StandardCharsets.UTF_8);
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            out[2 * i] = HEX[(bytes[i] >> 4) & 0xf];
            out[2 * i + 1] = HEX[bytes[i] & 0xf];
        }
        return new String(out);
    }
}
