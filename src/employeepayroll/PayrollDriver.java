package employeepayroll;

// ── Hadoop core imports ────────────────────────────────────────────────────
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// ── Project imports ────────────────────────────────────────────────────────
import employeepayroll.EmployeeMapper;
import employeepayroll.PayrollMapper;
import employeepayroll.PayrollReducer;

public class PayrollDriver extends Configured implements Tool {

    // ── Constants ────────────────────────────────────────────────────────────

    /** Human-readable name displayed in the YARN ResourceManager UI. */
    private static final String JOB_NAME = "Employee Payroll Analytics – Reduce-Side Join";

    /** Expected number of positional command-line arguments. */
    private static final int EXPECTED_ARG_COUNT = 3;

    /** Argument index for the employee directory input path. */
    private static final int ARG_EMPLOYEE_INPUT = 0;

    /** Argument index for the payroll transactions input path. */
    private static final int ARG_PAYROLL_INPUT  = 1;

    /** Argument index for the job output path. */
    private static final int ARG_OUTPUT_PATH    = 2;

    
    private static final int REDUCER_COUNT = 1;

    /** POSIX exit code indicating successful job completion. */
    private static final int EXIT_SUCCESS = 0;

    /** POSIX exit code indicating a configuration or execution failure. */
    private static final int EXIT_FAILURE = 1;

    // ────────────────────────────────────────────────────────────────────────
    //  Entry point
    // ────────────────────────────────────────────────────────────────────────

    
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new PayrollDriver(), args);
        System.exit(exitCode);
    }

    // ────────────────────────────────────────────────────────────────────────
    //  Tool implementation – run
    // ────────────────────────────────────────────────────────────────────────

    
    @Override
    public int run(String[] args) throws Exception {

        // ── Step 1: Validate argument count ───────────────────────────────────
        //   Fail fast with a clear usage message rather than letting Hadoop
        //   throw a cryptic NullPointerException later.
        if (args.length != EXPECTED_ARG_COUNT) {
            System.err.println("────────────────────────────────────────────────────");
            System.err.println("  ERROR: Incorrect number of arguments.");
            System.err.println("  Expected : " + EXPECTED_ARG_COUNT);
            System.err.println("  Received : " + args.length);
            System.err.println();
            System.err.println("  Usage:");
            System.err.println("    hadoop jar payroll-analytics.jar \\");
            System.err.println("      com.enterprise.hadoop.employeepayroll.driver.PayrollDriver \\");
            System.err.println("      <employeeInputPath> <payrollInputPath> <outputPath>");
            System.err.println("────────────────────────────────────────────────────");
            return EXIT_FAILURE;
        }

        // ── Step 2: Resolve HDFS path objects ─────────────────────────────────
        //   Using named variables (not inline args[n]) makes the configuration
        //   block below self-documenting and prevents index-off-by-one mistakes.
        Path employeeInputPath = new Path(args[ARG_EMPLOYEE_INPUT]);
        Path payrollInputPath  = new Path(args[ARG_PAYROLL_INPUT]);
        Path outputPath        = new Path(args[ARG_OUTPUT_PATH]);

        System.out.println("────────────────────────────────────────────────────");
        System.out.println("  Job          : " + JOB_NAME);
        System.out.println("  Employee In  : " + employeeInputPath);
        System.out.println("  Payroll In   : " + payrollInputPath);
        System.out.println("  Output       : " + outputPath);
        System.out.println("────────────────────────────────────────────────────");

        // ── Step 3: Create and name the Job ───────────────────────────────────
        //   getConf() returns the Configuration enriched by ToolRunner with
        //   cluster-specific settings and any -D overrides from the command line.
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, JOB_NAME);

        // Set the JAR that contains the Driver class.
        // Hadoop uses this to locate and distribute the user's code to all
        // cluster nodes before the job starts.
        job.setJarByClass(PayrollDriver.class);

        // ── Step 4: Register Mappers via MultipleInputs ───────────────────────
        //   MultipleInputs.addInputPath() binds a specific HDFS path to a
        //   specific (InputFormat, Mapper) pair.  This allows each input dataset
        //   to be parsed by the Mapper that understands its schema, while both
        //   mappers still emit the same output key type (Text / employeeId) so
        //   the shuffle can route matching records to the same Reducer.
        //
        //   Note: When MultipleInputs is used, do NOT call
        //   FileInputFormat.addInputPath() or job.setMapperClass() — those APIs
        //   conflict with the MultipleInputs registration.

        // Employee Directory → EmployeeMapper
        // Emits: (employeeId, "emp~firstName,lastName,department")
        MultipleInputs.addInputPath(
                job,
                employeeInputPath,
                TextInputFormat.class,
                EmployeeMapper.class
        );

        // Payroll Transactions → PayrollMapper
        // Emits: (employeeId, "pay~month,baseSalary,bonus")
        MultipleInputs.addInputPath(
                job,
                payrollInputPath,
                TextInputFormat.class,
                PayrollMapper.class
        );

        // ── Step 5: Configure the Reducer ─────────────────────────────────────
        //   PayrollReducer performs the in-memory join: it collects all "emp~"
        //   and "pay~" values for a given employeeId, then emits one enriched
        //   CSV line per valid payroll record.
        job.setReducerClass(PayrollReducer.class);

        // A single Reducer ensures all records for every employeeId converge in
        // one task and the output is written to a single part-file.  Increase
        // this for production workloads where the data exceeds a few GB.
        job.setNumReduceTasks(REDUCER_COUNT);

        // ── Step 6: Set Mapper output key/value types ─────────────────────────
        //   Hadoop needs the intermediate (Mapper output) types to configure
        //   the shuffle correctly.  These must match the generic type parameters
        //   declared in both EmployeeMapper and PayrollMapper.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // ── Step 7: Set final output key/value types ───────────────────────────
        //   The Reducer emits (Text key, Text value) pairs, written as
        //   tab-separated lines by TextOutputFormat.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // ── Step 8: Configure output format and output path ───────────────────
        //   TextOutputFormat writes one "key\tvalue" line per context.write()
        //   call, which produces a clean tab-separated output file.
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        // ── Step 9: Clean up pre-existing output directory ────────────────────
        //   Hadoop refuses to write to an output path that already exists.
        //   Deleting it programmatically prevents the job from failing on
        //   repeated test runs without requiring a manual "hdfs dfs -rm -r"
        //   command each time.
        //
        //   CAUTION: This permanently deletes the existing output.  In a
        //   production pipeline, consider archiving or versioning output paths
        //   instead of deleting them (e.g. append a timestamp to the path).
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputPath)) {
            System.out.println("  [WARN] Output path already exists. Deleting: " + outputPath);
            boolean deleted = hdfs.delete(outputPath, true); // true = recursive delete
            if (!deleted) {
                System.err.println("  [ERROR] Failed to delete existing output path: " + outputPath);
                return EXIT_FAILURE;
            }
            System.out.println("  [INFO] Output path deleted successfully.");
        }

        // ── Step 10: Submit the job and wait for completion ───────────────────
        //   waitForCompletion(true) prints progress to stdout and blocks until
        //   the job finishes.  It returns true on success, false on failure.
        System.out.println("  [INFO] Submitting job to cluster...");
        System.out.println("────────────────────────────────────────────────────");

        boolean jobSucceeded = job.waitForCompletion(true);

        // ── Step 11: Report outcome and return exit code ──────────────────────
        System.out.println("────────────────────────────────────────────────────");
        if (jobSucceeded) {
            System.out.println("  [SUCCESS] Job completed. Output written to: " + outputPath);
            System.out.println("────────────────────────────────────────────────────");
            return EXIT_SUCCESS;
        } else {
            System.err.println("  [FAILURE] Job did not complete successfully.");
            System.err.println("  Check the YARN Resource Manager UI for details.");
            System.err.println("────────────────────────────────────────────────────");
            return EXIT_FAILURE;
        }
    }
}