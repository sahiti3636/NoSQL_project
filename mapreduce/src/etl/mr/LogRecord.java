package etl.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * One parsed log line, as ParseJob stores it for the query jobs (SequenceFile values).
 *
 * A binary Writable instead of a text format: 7 real request lines contain byte 0x01, and a path
 * could contain a tab, so no delimiter is safe. Strings are written with Text.writeString (UTF-8,
 * any length). Missing parts are '' (never null), as the contract requires.
 *
 * Q2/Q3 jobs (Tushaar): read the parsed folders with SequenceFileInputFormat; the mapper gets
 * (NullWritable, LogRecord). Use resourcePath, host, logDate, logHour, statusCode, bytes.
 */
public class LogRecord implements Writable {
    public String host = "";
    public String logDate = "";          // YYYY-MM-DD, built by slicing the timestamp
    public int logHour;                  // 0-23, as logged (no timezone conversion)
    public String httpMethod = "";
    public String resourcePath = "";
    public String protocolVersion = "";
    public int statusCode;
    public long bytes;                   // '-' -> 0

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, host);
        Text.writeString(out, logDate);
        out.writeInt(logHour);
        Text.writeString(out, httpMethod);
        Text.writeString(out, resourcePath);
        Text.writeString(out, protocolVersion);
        out.writeInt(statusCode);
        out.writeLong(bytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        host = Text.readString(in);
        logDate = Text.readString(in);
        logHour = in.readInt();
        httpMethod = Text.readString(in);
        resourcePath = Text.readString(in);
        protocolVersion = Text.readString(in);
        statusCode = in.readInt();
        bytes = in.readLong();
    }

    /** All fields as one COPY-format line, e.g. for `hadoop fs -text` when debugging. */
    @Override
    public String toString() {
        return CopyText.row(host, logDate, logHour, httpMethod, resourcePath, protocolVersion, statusCode, bytes);
    }

    public boolean isError() {
        return statusCode >= 400 && statusCode <= 599;
    }
}
