package etl.mr;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

/**
 * Person B (Tushaar): whole-run counts for finalize().
 *
 * Counts the rows kept by every batch's ParseJob, inside MapReduce (not in Python):
 *   valid      = LogRecords in mr_data/parsed/batch_*      (SequenceFiles)
 *   malformed  = lines in mr_data/malformed/batch_*        (COPY text, one escaped line per record,
 *                                                           so a raw \n or \r never splits a record)
 * Map-only job; the totals come from job counters. Writes one line "valid<TAB>malformed".
 *
 * Usage: hadoop jar etl-mr.jar etl.mr.CountJob <parsed glob> <malformed glob> <counts file>
 */
public class CountJob {

    enum Count { VALID, MALFORMED }

    public static class ParsedMapper extends Mapper<NullWritable, LogRecord, NullWritable, NullWritable> {
        @Override
        protected void map(NullWritable k, LogRecord r, Context ctx) {
            ctx.getCounter(Count.VALID).increment(1);
        }
    }

    public static class MalformedMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
        @Override
        protected void map(LongWritable offset, Text line, Context ctx) {
            ctx.getCounter(Count.MALFORMED).increment(1);
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(run(args));
    }

    /** The job itself; returns 0 on success. RunJobs calls this to run several jobs in one JVM. */
    public static int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("usage: CountJob <parsed glob> <malformed glob> <counts file>");
            return 2;
        }
        Configuration conf = Jobs.conf();
        conf.set("textinputformat.record.delimiter", "\n");
        Job job = Job.getInstance(conf, "etl whole-run counts");
        job.setJarByClass(CountJob.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), SequenceFileInputFormat.class, ParsedMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MalformedMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        if (!job.waitForCompletion(false)) {
            System.err.println("CountJob failed: " + job.getStatus().getFailureInfo());
            return 1;
        }
        Counters n = job.getCounters();
        long valid = n.findCounter(Count.VALID).getValue();
        long malformed = n.findCounter(Count.MALFORMED).getValue();
        Files.write(Paths.get(args[2]), (CopyText.row(valid, malformed) + "\n").getBytes(StandardCharsets.UTF_8));
        return 0;
    }
}
