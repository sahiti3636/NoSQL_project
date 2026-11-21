package etl.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Setup check only (scripts/check_phase2.sh), not part of the pipeline.
 * Counts the lines of an input folder and how many match the contract envelope regex, which
 * proves Hadoop, the compiler, the /runs mount and the /contracts mount all work.
 *
 * Usage: hadoop jar etl-mr.jar etl.mr.LineCount <input dir> <output dir>
 * Output: one line "lines<TAB>envelope_matches".
 */
public class LineCount {

    public static class Map extends Mapper<LongWritable, Text, NullWritable, LongWritable> {
        private Contracts contracts;
        private long lines, matches;

        @Override
        protected void setup(Context ctx) throws IOException {
            contracts = Contracts.load(ctx.getConfiguration().get("etl.contracts.dir", Contracts.DEFAULT_DIR));
        }

        @Override
        protected void map(LongWritable offset, Text line, Context ctx) {
            String s = line.toString();                 // UTF-8, invalid bytes -> U+FFFD (contract)
            if (s.endsWith("\r")) s = s.substring(0, s.length() - 1);
            lines++;
            if (contracts.envelope.matcher(s).matches()) matches++;
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            ctx.write(NullWritable.get(), new LongWritable(lines));
            ctx.write(NullWritable.get(), new LongWritable(-1 - matches));   // negative = match count
        }
    }

    public static class Sum extends Reducer<NullWritable, LongWritable, Text, NullWritable> {
        @Override
        protected void reduce(NullWritable k, Iterable<LongWritable> values, Context ctx)
                throws IOException, InterruptedException {
            long lines = 0, matches = 0;
            for (LongWritable v : values) {
                if (v.get() >= 0) lines += v.get(); else matches += -1 - v.get();
            }
            ctx.write(new Text(CopyText.row(lines, matches)), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // Contract: lines end at \n only (Hadoop's default would also split on a lone \r).
        conf.set("textinputformat.record.delimiter", "\n");
        Job job = Job.getInstance(conf, "etl line count check");
        job.setJarByClass(LineCount.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Sum.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
