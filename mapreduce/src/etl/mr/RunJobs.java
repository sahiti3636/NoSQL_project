package etl.mr;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

/**
 * Runs several jobs one after another in ONE JVM and records how long each took.
 *
 * Starting `hadoop jar` costs about a second (JVM start, Hadoop setup) before a job does any
 * work. A batch runs four jobs (parse, Q1, Q2, Q3), so the engine starts them all from one
 * `hadoop jar` call and still gets one time per query for etl.query_execution.
 *
 * Usage: hadoop jar etl-mr.jar etl.mr.RunJobs <plan file> <timings file>
 *   plan file:    one step per line, tab-separated: label, class (e.g. Q1Job), arguments...
 *   timings file: one line per finished step: label TAB milliseconds
 * Each step calls the class's static run(String[]) method. The first step that fails stops the
 * run with that step's exit code, so a failed query never looks like a finished one.
 */
public final class RunJobs {
    private RunJobs() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("usage: RunJobs <plan file> <timings file>");
            System.exit(2);
        }
        List<String> plan = Files.readAllLines(Paths.get(args[0]), StandardCharsets.UTF_8);
        Files.write(Paths.get(args[1]), new byte[0]);
        for (String line : plan) {
            if (line.isEmpty()) continue;
            String[] f = line.split("\t");
            String label = f[0];
            Method run = Class.forName("etl.mr." + f[1]).getMethod("run", String[].class);
            String[] jobArgs = Arrays.copyOfRange(f, 2, f.length);

            long t0 = System.nanoTime();
            int rc;
            try {
                rc = (Integer) run.invoke(null, (Object) jobArgs);
            } catch (InvocationTargetException e) {
                System.err.println("step " + label + " (" + f[1] + ") threw:");
                e.getCause().printStackTrace();
                rc = 1;
            }
            long ms = (System.nanoTime() - t0) / 1_000_000;
            if (rc != 0) {
                System.err.println("step " + label + " (" + f[1] + ") failed with exit code " + rc);
                System.exit(rc);
            }
            Files.write(Paths.get(args[1]), (label + "\t" + ms + "\n").getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.APPEND);
            System.err.println("step " + label + " done in " + ms + " ms");
        }
    }
}
