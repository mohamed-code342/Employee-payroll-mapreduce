package employeepayroll;

// ── Hadoop core imports ────────────────────────────────────────────────────
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

// ── Java standard-library imports ─────────────────────────────────────────
import java.io.IOException;


public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text> {

    // ── Constants ────────────────────────────────────────────────────────────

    /**
     * Delimiter used to separate fields in the input CSV file.
     * Defined as a constant so it can be changed in one place if the
     * source format ever migrates to a different separator (e.g. tab).
     */
    private static final String FIELD_DELIMITER = ",";

    /**
     * Tag prepended to every value emitted by this mapper.
     * The Reducer uses this prefix to identify employee-side records
     * when merging with payroll records tagged with "pay~".
     */
    private static final String EMPLOYEE_TAG = "emp~";

    /**
     * Expected number of fields in a well-formed employee record.
     * Records with a different field count are considered malformed
     * and are silently dropped (counter incremented for observability).
     */
    private static final int EXPECTED_FIELD_COUNT = 4;

    /**
     * Column indices – named constants improve readability and make
     * schema changes a single-line edit.
     */
    private static final int COL_EMPLOYEE_ID = 0;
    private static final int COL_FIRST_NAME  = 1;
    private static final int COL_LAST_NAME   = 2;
    private static final int COL_DEPARTMENT  = 3;

    // ── Reusable output objects ──────────────────────────────────────────────
    //
    // Declaring these as instance variables (initialised once in setup())
    // prevents repeated heap allocation inside the hot map() loop.
    // On large datasets this measurably reduces GC pressure.

    /** Reusable Text object written as the output key (employeeId). */
    private Text outputKey;

    /** Reusable Text object written as the output value (tagged payload). */
    private Text outputValue;

    // ── Hadoop Counters ──────────────────────────────────────────────────────

    /**
     * Enum grouping all counters emitted by this mapper.
     * Values appear in the job history server and YARN UI.
     */
    public enum EmployeeCounters {
        /** Total lines read from the input split (including header / blank). */
        LINES_READ,
        /** Lines successfully validated and emitted. */
        RECORDS_EMITTED,
        /** Lines dropped because they are blank or contain only whitespace. */
        BLANK_LINES_SKIPPED,
        /** Lines dropped because the field count ≠ {@value #EXPECTED_FIELD_COUNT}. */
        MALFORMED_RECORDS_SKIPPED,
        /** Lines dropped because one or more required fields are empty after trimming. */
        INCOMPLETE_RECORDS_SKIPPED
    }

    // ────────────────────────────────────────────────────────────────────────
    //  Lifecycle – setup
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Called once per Mapper task before any records are processed.
     *
     * <p>Initialises the reusable {@link Text} output objects.  Using
     * {@code setup()} for one-time initialisation keeps {@code map()}
     * free of object-creation overhead.
     *
     * @param context the Mapper context provided by the Hadoop framework
     * @throws IOException          propagated from the base class
     * @throws InterruptedException propagated from the base class
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        outputKey   = new Text();
        outputValue = new Text();
    }

    // ────────────────────────────────────────────────────────────────────────
    //  Core – map
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Processes one line of the Employee Directory file.
     *
     * <p><b>Processing pipeline:</b>
     * <ol>
     *   <li>Increment the {@code LINES_READ} counter.</li>
     *   <li>Convert the Hadoop {@link Text} value to a Java {@link String}
     *       and trim surrounding whitespace.</li>
     *   <li>Skip blank / empty lines → {@code BLANK_LINES_SKIPPED}.</li>
     *   <li>Split on the comma delimiter.</li>
     *   <li>Validate field count == 4 → {@code MALFORMED_RECORDS_SKIPPED}.</li>
     *   <li>Trim each individual field.</li>
     *   <li>Validate that no required field is empty after trimming
     *       → {@code INCOMPLETE_RECORDS_SKIPPED}.</li>
     *   <li>Build the tagged payload string.</li>
     *   <li>Set the reusable output objects and call {@code context.write()}.</li>
     *   <li>Increment {@code RECORDS_EMITTED}.</li>
     * </ol>
     *
     * @param key     byte offset of the current line (unused, but required
     *                by the Mapper contract)
     * @param value   raw text line delivered by the InputFormat
     * @param context MapReduce context used for writing output and counters
     * @throws IOException          if the underlying write fails
     * @throws InterruptedException if the task is interrupted by the framework
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // ── Step 1: Count every line seen (for data-quality reporting) ───────
        context.getCounter(EmployeeCounters.LINES_READ).increment(1);

        // ── Step 2: Convert to String and strip leading/trailing whitespace ──
        //   Text.toString() is a lightweight operation; no new char[] is copied
        //   until we call trim(), so this is acceptably efficient.
        String rawLine = value.toString().trim();

        // ── Step 3: Skip blank lines ─────────────────────────────────────────
        //   Blank lines are common at the end of files or after header rows.
        if (rawLine.isEmpty()) {
            context.getCounter(EmployeeCounters.BLANK_LINES_SKIPPED).increment(1);
            return;
        }

        // ── Step 4: Split on comma ───────────────────────────────────────────
        //   The -1 limit preserves trailing empty fields so that records like
        //   "EMP01,Nour,Hassan," are counted as 4 fields (the last being empty)
        //   rather than silently collapsed to 3 fields by the default split.
        String[] fields = rawLine.split(FIELD_DELIMITER, -1);

        // ── Step 5: Validate field count ─────────────────────────────────────
        if (fields.length != EXPECTED_FIELD_COUNT) {
            context.getCounter(EmployeeCounters.MALFORMED_RECORDS_SKIPPED).increment(1);
            return;
        }

        // ── Step 6: Trim individual fields ───────────────────────────────────
        //   Source files sometimes have spaces around commas ("EMP01 , Nour").
        //   Trimming ensures clean values regardless of source formatting.
        String employeeId  = fields[COL_EMPLOYEE_ID].trim();
        String firstName   = fields[COL_FIRST_NAME].trim();
        String lastName    = fields[COL_LAST_NAME].trim();
        String department  = fields[COL_DEPARTMENT].trim();

        // ── Step 7: Validate that no required field is empty ─────────────────
        //   An employee record with a missing ID or department cannot be
        //   meaningfully joined and is therefore discarded.
        if (employeeId.isEmpty() || firstName.isEmpty()
                || lastName.isEmpty() || department.isEmpty()) {
            context.getCounter(EmployeeCounters.INCOMPLETE_RECORDS_SKIPPED).increment(1);
            return;
        }

        // ── Step 8: Build the tagged payload ─────────────────────────────────
        //   Format:  emp~<firstName>,<lastName>,<department>
        //   Example: emp~Nour,Hassan,Engineering
        //
        //   StringBuilder is preferred over string concatenation (+) in a loop
        //   to avoid creating intermediate String objects.
        String taggedPayload = new StringBuilder()
                .append(EMPLOYEE_TAG)
                .append(firstName)
                .append(FIELD_DELIMITER)
                .append(lastName)
                .append(FIELD_DELIMITER)
                .append(department)
                .toString();

        // ── Step 9: Populate reusable output objects and emit ────────────────
        //   Text.set(String) reuses the underlying byte array when the new
        //   string fits, avoiding a heap allocation on every record.
        outputKey.set(employeeId);
        outputValue.set(taggedPayload);

        context.write(outputKey, outputValue);

        // ── Step 10: Track successfully emitted records ───────────────────────
        context.getCounter(EmployeeCounters.RECORDS_EMITTED).increment(1);
    }

    // ────────────────────────────────────────────────────────────────────────
    //  Lifecycle – cleanup
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Called once per Mapper task after all records have been processed.
     *
     * <p>Currently a no-op placeholder.  Override this method if resources
     * such as database connections or file handles need to be released.
     *
     * @param context the Mapper context provided by the Hadoop framework
     * @throws IOException          propagated from the base class
     * @throws InterruptedException propagated from the base class
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        // No external resources to release in this implementation.
        // Add resource teardown here if connections are opened in setup().
    }
}