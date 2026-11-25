package etl.mr;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Person A (Sahiti): ingestion, parsing and malformed records for one batch. Map-only job.
 *
 * Reads every raw line of the batch folder (1 or 2 unchanged pieces of the logs), parses it with
 * the contract regexes and month map, and writes
 *   - valid lines   as LogRecord SequenceFiles  -> parsedDir     (kept for the query jobs and finalize)
 *   - invalid lines as COPY-format raw text     -> malformedDir  (kept for finalize)
 *                                                  and malformedTsv (this batch's malformed.tsv)
 *   - one row "records_read  valid  malformed"  -> countsFile
 *
 * Parsing follows the Hive engine exactly (Matcher.find(), as Hive's RLIKE and regexp_extract do):
 *   malformed  = envelope does not match, or the month is not in months.tsv
 *   log_date   = year-month-day by slicing the timestamp (no date functions, no timezone shift)
 *   log_timestamp = log_date + " HH:MM:SS +hhmm", also sliced (the time as logged, zone kept)
 *   request    = request regex (method, path, protocol) or, when it does not match, the fallback
 *                regex gives the path and method/protocol are ''
 *   bytes '-'  = 0
 *
 * Usage: hadoop jar etl-mr.jar etl.mr.ParseJob <input> <workDir> <parsedDir> <malformedDir>
 *                                               <malformedTsv> <countsFile>
 */
public class ParseJob {

    enum Count { READ, VALID, MALFORMED }

    public static class ParseMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
        private Contracts c;
        private MultipleOutputs<NullWritable, NullWritable> out;
        private final LogRecord rec = new LogRecord();
        private final Text raw = new Text();

        @Override
        protected void setup(Context ctx) throws IOException {
            c = Contracts.load(ctx.getConfiguration().get("etl.contracts.dir", Contracts.DEFAULT_DIR));
            out = new MultipleOutputs<>(ctx);
        }

        @Override
        protected void map(LongWritable offset, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();                     // UTF-8; invalid bytes -> U+FFFD
            if (line.endsWith("\r")) line = line.substring(0, line.length() - 1);
            ctx.getCounter(Count.READ).increment(1);

            Matcher m = c.envelope.matcher(line);
            String month = m.find() ? c.months.get(m.group(5)) : null;
            if (month == null) {
                raw.set(CopyText.row(line));
                out.write("malformed", raw, NullWritable.get());
                ctx.getCounter(Count.MALFORMED).increment(1);
                return;
            }

            rec.host = m.group(1);
            rec.logDate = m.group(6) + "-" + month + "-" + m.group(4);
            rec.logTimestamp = rec.logDate + " " + m.group(7) + ":" + m.group(8) + ":" + m.group(9) + " " + m.group(10);
            rec.logHour = Integer.parseInt(m.group(7));
            String request = m.group(11);
            Matcher r = c.request.matcher(request);
            if (r.find()) {
                rec.httpMethod = r.group(1);
                rec.resourcePath = r.group(2);
                rec.protocolVersion = r.group(3) == null ? "" : r.group(3);
            } else {
                Matcher f = c.requestFallback.matcher(request);
                rec.httpMethod = "";
                rec.resourcePath = f.find() ? f.group(1) : request;
                rec.protocolVersion = "";
            }
            rec.statusCode = Integer.parseInt(m.group(12));
            String bytes = m.group(13);
            rec.bytes = "-".equals(bytes) ? 0L : Long.parseLong(bytes);

            out.write("parsed", NullWritable.get(), rec);
            ctx.getCounter(Count.VALID).increment(1);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            out.close();
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(run(args));
    }

    /** The job itself; returns 0 on success. RunJobs calls this to run several jobs in one JVM. */
    public static int run(String[] args) throws Exception {
        if (args.length != 6) {
            System.err.println("usage: ParseJob <input> <workDir> <parsedDir> <malformedDir> <malformedTsv> <countsFile>");
            return 2;
        }
        Path input = new Path(args[0]), work = new Path(args[1]);
        Path parsedDir = new Path(args[2]), malformedDir = new Path(args[3]), malformedTsv = new Path(args[4]);

        Configuration conf = Jobs.conf();
        conf.set("textinputformat.record.delimiter", "\n");   // contract: a lone \r is not a line end
        Job job = Job.getInstance(conf, "etl parse " + input.getName());
        job.setJarByClass(ParseJob.class);
        job.setMapperClass(ParseMapper.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, work);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);   // no empty part-m files
        MultipleOutputs.addNamedOutput(job, "parsed", SequenceFileOutputFormat.class,
                NullWritable.class, LogRecord.class);
        MultipleOutputs.addNamedOutput(job, "malformed", TextOutputFormat.class,
                Text.class, NullWritable.class);
        if (!job.waitForCompletion(false)) {
            System.err.println("ParseJob failed: " + job.getStatus().getFailureInfo());
            return 1;
        }

        // Move the outputs to where the query jobs and finalize() expect them.
        FileSystem fs = FileSystem.getLocal(conf);
        fs.mkdirs(parsedDir);
        fs.mkdirs(malformedDir);
        fs.mkdirs(malformedTsv);
        for (FileStatus f : fs.listStatus(work)) {
            String name = f.getPath().getName();
            if (name.startsWith("parsed-")) {
                fs.rename(f.getPath(), new Path(parsedDir, name));
            } else if (name.startsWith("malformed-")) {
                FileUtil.copy(fs, f.getPath(), fs, new Path(malformedTsv, name), false, conf);
                fs.rename(f.getPath(), new Path(malformedDir, name));
            }
        }
        fs.delete(work, true);

        Counters n = job.getCounters();
        long read = n.findCounter(Count.READ).getValue();
        long valid = n.findCounter(Count.VALID).getValue();
        long malformed = n.findCounter(Count.MALFORMED).getValue();
        Files.write(Paths.get(args[5]), (CopyText.row(read, valid, malformed) + "\n").getBytes(StandardCharsets.UTF_8));
        return 0;
    }
}
