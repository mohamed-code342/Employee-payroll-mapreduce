package employeepayroll;

// ── Hadoop core imports ────────────────────────────────────────────────────
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// ── Java standard-library imports ─────────────────────────────────────────
import java.io.IOException;

public class PayrollMapper extends Mapper<LongWritable, Text, Text, Text> {

    // ── Constants ────────────────────────────────────────────────────────────

    /**
     * Delimiter used to separate fields in the input CSV file.
     * Centralised here so a format migration (e.g. to TSV) requires
     * only a one-line change.
     */
    private static final String FIELD_DELIMITER = ",";

    /**
     * Dataset tag prepended to every value emitted by this mapper.
     * The Reducer identifies payroll-side records by this prefix and
     * routes them to the correct join branch.
     */
    private static final String PAYROLL_TAG = "pay~";

    /**
     * Exact number of comma-separated fields expected per record.
     * Records with a different count are treated as malformed.
     */
    private static final int EXPECTED_FIELD_COUNT = 5;

    /**
     * Minimum acceptable value for both baseSalary and bonus.
     * Negative compensation figures indicate corrupt or erroneous data.
     */
    private static final int MIN_NUMERIC_VALUE = 0;

    // ── Column index constants ───────────────────────────────────────────────
    //   Named constants eliminate magic numbers and make schema changes trivial.

    private static final int COL_PAYROLL_ID   = 0;
    private static final int COL_EMPLOYEE_ID  = 1;
    private static final int COL_MONTH        = 2;
    private static final int COL_BASE_SALARY  = 3;
    private static final int COL_BONUS        = 4;

    // ── Reusable output objects ──────────────────────────────────────────────
    //
    //   Instantiated once in setup() and reused across all map() calls.
    //   Text.set(String) reuses the internal byte array when the new value
    //   fits, avoiding heap allocation on every record — critical at scale.

    /** Reusable container for the output key (employeeId). */
    private Text outputKey;

    /** Reusable container for the output value (tagged payroll payload). */
    private Text outputValue;

    // ── Hadoop Counters ──────────────────────────────────────────────────────

    /**
     * Counter group for PayrollMapper data-quality metrics.
     * All values are visible in the YARN ResourceManager UI and
     * in the job history server after the job completes.
     */
    public enum PayrollCounters {
        /** Every line read from the input split, regardless of validity. */
        LINES_READ,
        /** Records that passed all validation checks and were emitted. */
        RECORDS_EMITTED,
        /** Lines that were blank or contained only whitespace. */
        BLANK_LINES_SKIPPED,
        /** Lines whose field count differed from {@value #EXPECTED_FIELD_COUNT}. */
        MALFORMED_RECORDS_SKIPPED,
        /** Records where one or more required string fields were empty after trim. */
        INCOMPLETE_RECORDS_SKIPPED,
        /** Records where baseSalary or bonus could not be parsed as an integer. */
        INVALID_NUMERIC_FIELDS_SKIPPED,
        /** Records where baseSalary or bonus was negative. */
        NEGATIVE_VALUE_RECORDS_SKIPPED
    }

    // ────────────────────────────────────────────────────────────────────────
    //  Lifecycle – setup
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Invoked once per Mapper task before any records are processed.
     *
     * <p>Creates the reusable {@link Text} output objects so that
     * {@code map()} never allocates new instances during record processing.
     *
     * @param context Hadoop task context supplied by the framework
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
     * Processes one input line from the Payroll Transactions file.
     *
     * <p><b>Validation pipeline (fail-fast, ordered by cheapness):</b>
     * <ol>
     *   <li>Increment {@code LINES_READ}.</li>
     *   <li>Convert {@link Text} → {@link String}, trim whitespace.</li>
     *   <li>Reject blank lines  → {@code BLANK_LINES_SKIPPED}.</li>
     *   <li>Split on comma with limit -1 (preserves trailing empty fields).</li>
     *   <li>Reject wrong field count → {@code MALFORMED_RECORDS_SKIPPED}.</li>
     *   <li>Trim individual fields.</li>
     *   <li>Reject empty required string fields
     *       → {@code INCOMPLETE_RECORDS_SKIPPED}.</li>
     *   <li>Parse baseSalary and bonus as integers; reject non-numeric values
     *       → {@code INVALID_NUMERIC_FIELDS_SKIPPED}.</li>
     *   <li>Reject negative numeric values
     *       → {@code NEGATIVE_VALUE_RECORDS_SKIPPED}.</li>
     *   <li>Build tagged payload, populate reusable objects, emit.</li>
     *   <li>Increment {@code RECORDS_EMITTED}.</li>
     * </ol>
     *
     * @param key     byte offset of the current line (unused join key)
     * @param value   raw text line from the InputFormat
     * @param context MapReduce context for output and counter updates
     * @throws IOException          if the underlying write operation fails
     * @throws InterruptedException if the task is interrupted by the framework
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // ── Step 1: Count every line read for data-quality observability ──────
        context.getCounter(PayrollCounters.LINES_READ).increment(1);

        // ── Step 2: Convert to String and remove surrounding whitespace ───────
        String rawLine = value.toString().trim();

        // ── Step 3: Discard blank lines ───────────────────────────────────────
        //   Blank lines appear at the end of files, between sections, or as
        //   artefacts of certain export tools.  They carry no data.
        if (rawLine.isEmpty()) {
            context.getCounter(PayrollCounters.BLANK_LINES_SKIPPED).increment(1);
            return;
        }

        // ── Step 4: Tokenise on the comma delimiter ───────────────────────────
        //   The -1 limit ensures trailing empty tokens are preserved, e.g.
        //   "PAY001,EMP01,Jan,8000," → 5 fields (last one empty), not 4.
        String[] fields = rawLine.split(FIELD_DELIMITER, -1);

        // ── Step 5: Validate field count ─────────────────────────────────────
        //   Records with too few or too many fields cannot be reliably parsed.
        if (fields.length != EXPECTED_FIELD_COUNT) {
            context.getCounter(PayrollCounters.MALFORMED_RECORDS_SKIPPED).increment(1);
            return;
        }

        // ── Step 6: Trim each individual field ───────────────────────────────
        //   Some source systems pad fields with spaces around the delimiter,
        //   e.g. "EMP01 , Jan , 8000".  Trimming ensures clean tokens.
        String payrollId   = fields[COL_PAYROLL_ID].trim();
        String employeeId  = fields[COL_EMPLOYEE_ID].trim();
        String month       = fields[COL_MONTH].trim();
        String salaryRaw   = fields[COL_BASE_SALARY].trim();
        String bonusRaw    = fields[COL_BONUS].trim();

        // ── Step 7: Validate that required string fields are non-empty ────────
        //   An employee ID is mandatory for the join key; month and payrollId
        //   are required for meaningful downstream analytics.
        if (payrollId.isEmpty() || employeeId.isEmpty()
                || month.isEmpty() || salaryRaw.isEmpty() || bonusRaw.isEmpty()) {
            context.getCounter(PayrollCounters.INCOMPLETE_RECORDS_SKIPPED).increment(1);
            return;
        }

        // ── Step 8: Parse and validate numeric fields ─────────────────────────
        //   Integer.parseInt() is used instead of a regex because it is faster
        //   and directly produces the int value needed for the range check below.
        //   A NumberFormatException indicates non-numeric data (e.g. "N/A", "–").
        int baseSalary;
        int bonus;

        try {
            baseSalary = Integer.parseInt(salaryRaw);
            bonus      = Integer.parseInt(bonusRaw);
        } catch (NumberFormatException e) {
            // baseSalary or bonus is not a valid integer — discard the record.
            context.getCounter(PayrollCounters.INVALID_NUMERIC_FIELDS_SKIPPED).increment(1);
            return;
        }

        // ── Step 9: Enforce non-negative business rule ────────────────────────
        //   Negative compensation values indicate upstream data corruption.
        //   Such records are excluded from the join to protect report accuracy.
        if (baseSalary < MIN_NUMERIC_VALUE || bonus < MIN_NUMERIC_VALUE) {
            context.getCounter(PayrollCounters.NEGATIVE_VALUE_RECORDS_SKIPPED).increment(1);
            return;
        }

        // ── Step 10: Build the tagged payroll payload ─────────────────────────
        //   Format:  pay~<month>,<baseSalary>,<bonus>
        //   Example: pay~Jan,8000,500
        //
        //   StringBuilder avoids intermediate String objects from the "+" operator.
        //   We append the already-validated integer values (not the raw strings)
        //   to guarantee clean numeric output even if the source had extra spaces.
        String taggedPayload = new StringBuilder()
                .append(PAYROLL_TAG)
                .append(month)
                .append(FIELD_DELIMITER)
                .append(baseSalary)     // validated int — no risk of dirty string
                .append(FIELD_DELIMITER)
                .append(bonus)          // validated int — no risk of dirty string
                .toString();

        // ── Step 11: Set reusable objects and emit the key-value pair ─────────
        //   Text.set(String) reuses the internal byte array when capacity allows,
        //   keeping garbage-collection pressure minimal across millions of records.
        outputKey.set(employeeId);
        outputValue.set(taggedPayload);

        context.write(outputKey, outputValue);

        // ── Step 12: Track successfully emitted records ───────────────────────
        context.getCounter(PayrollCounters.RECORDS_EMITTED).increment(1);
    }

    // ────────────────────────────────────────────────────────────────────────
    //  Lifecycle – cleanup
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Invoked once per Mapper task after all records have been processed.
     *
     * <p>Placeholder for resource teardown (e.g. closing a database connection
     * or a buffered writer opened in {@code setup()}).  Currently a no-op
     * because this mapper holds no external resources.
     *
     * @param context Hadoop task context supplied by the framework
     * @throws IOException          propagated from the base class
     * @throws InterruptedException propagated from the base class
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        // No external resources to release in this implementation.
    }
}