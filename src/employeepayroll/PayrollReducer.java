package employeepayroll;

// ── Hadoop core imports ────────────────────────────────────────────────────
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// ── Java standard-library imports ─────────────────────────────────────────
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class PayrollReducer extends Reducer<Text, Text, Text, Text> {

    // ── Constants ────────────────────────────────────────────────────────────

    /** Prefix that identifies an employee directory value. */
    private static final String EMPLOYEE_TAG = "emp~";

    /** Prefix that identifies a payroll transaction value. */
    private static final String PAYROLL_TAG  = "pay~";

    /** Delimiter used in both input payloads and the output CSV line. */
    private static final String DELIMITER = ",";

    /** Full-name placeholder when no employee record accompanies the payroll data. */
    private static final String UNKNOWN_EMPLOYEE = "UNKNOWN EMPLOYEE";

    /** Department placeholder when no employee record is present. */
    private static final String UNKNOWN_DEPARTMENT = "UNKNOWN";

    
    private static final int INITIAL_MAX_PAY = Integer.MIN_VALUE;

    // ── Reusable output objects ──────────────────────────────────────────────

    /** Reusable Text object for the output value to avoid per-record allocation. */
    private Text outputValue;

    /**
     * Reusable StringBuilder for constructing output lines.
     * Reset with {@code setLength(0)} before each use.
     */
    private StringBuilder lineBuilder;

    // ── Hadoop Counters ──────────────────────────────────────────────────────

    /**
     * Counter group surfacing data-quality and join-quality metrics
     * in the YARN job history UI.
     */
    public enum PayrollReducerCounters {
        /** Total number of unique employeeId keys processed. */
        KEYS_PROCESSED,
        /** Output lines successfully written (one per valid payroll record). */
        RECORDS_EMITTED,
        /** Keys that arrived with payroll data but no matching employee record. */
        MISSING_EMPLOYEE_RECORDS,
        /** Keys that had an employee record but zero valid payroll records. */
        NO_PAYROLL_RECORDS,
        /** Payroll value strings that could not be parsed (wrong format or non-numeric). */
        MALFORMED_PAYROLL_VALUES,
        /** Value strings whose tag prefix was neither "emp~" nor "pay~". */
        UNRECOGNISED_TAG_SKIPPED
    }

    // ────────────────────────────────────────────────────────────────────────
    //  Lifecycle – setup
    // ────────────────────────────────────────────────────────────────────────

   
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        outputValue  = new Text();
        lineBuilder  = new StringBuilder();
    }

    // ────────────────────────────────────────────────────────────────────────
    //  Inner class – PayrollRecord
    // ────────────────────────────────────────────────────────────────────────

    
    private static final class PayrollRecord {

        /** Calendar month of this payroll entry (e.g. "Jan"). */
        final String month;

        /** Base salary component (validated non-negative integer). */
        final int baseSalary;

        /** Bonus component (validated non-negative integer). */
        final int bonus;

        /** Pre-computed total pay (baseSalary + bonus). */
        final int totalPay;

        /**
         * Constructs a PayrollRecord with all fields set.
         *
         * @param month      calendar month label
         * @param baseSalary validated base salary
         * @param bonus      validated bonus
         */
        PayrollRecord(String month, int baseSalary, int bonus) {
            this.month      = month;
            this.baseSalary = baseSalary;
            this.bonus      = bonus;
            this.totalPay   = baseSalary + bonus;
        }
    }

    // ────────────────────────────────────────────────────────────────────────
    //  Core – reduce
    // ────────────────────────────────────────────────────────────────────────

    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // ── Counter: track every unique employeeId key received ───────────────
        context.getCounter(PayrollReducerCounters.KEYS_PROCESSED).increment(1);

        // ── Join state initialisation ─────────────────────────────────────────

        // Employee fields — defaulted to UNKNOWN in case no "emp~" value arrives.
        // This handles orphaned payroll records gracefully without crashing.
        String fullName   = UNKNOWN_EMPLOYEE;
        String department = UNKNOWN_DEPARTMENT;
        boolean employeeFound = false;

        // Buffer for validated payroll records (Pass 1 → Pass 2).
        // ArrayList is appropriate: sequential write in Pass 1, sequential
        // read in Pass 2; no random access or removal required.
        List<PayrollRecord> payrollRecords = new ArrayList<>();

        // Running maximum totalPay across all payroll records for this employee.
        // Initialised to MIN_VALUE so any real totalPay will replace it.
        int maxPay = INITIAL_MAX_PAY;

        // ═════════════════════════════════════════════════════════════════════
        //  PASS 1 — Iterate the value Iterable exactly once
        // ─────────────────────────────────────────────────────────────────────
        //  IMPORTANT: Hadoop may reuse the same Text object across iterations
        //  for efficiency.  We call value.toString() immediately to capture
        //  the string content before the framework overwrites the object.
        // ═════════════════════════════════════════════════════════════════════

        for (Text value : values) {

            // Snapshot the current value before Hadoop can reuse the Text object.
            String taggedValue = value.toString();

            // ── Branch A: Employee directory record ───────────────────────────
            if (taggedValue.startsWith(EMPLOYEE_TAG)) {

                // Strip the "emp~" tag to obtain the raw payload.
                // Expected payload: firstName,lastName,department
                String empPayload = taggedValue.substring(EMPLOYEE_TAG.length());
                String[] empFields = empPayload.split(DELIMITER, -1);

                // Validate: exactly 3 fields are required.
                if (empFields.length != 3
                        || empFields[0].trim().isEmpty()
                        || empFields[1].trim().isEmpty()
                        || empFields[2].trim().isEmpty()) {

                    // Malformed employee value — fall back to UNKNOWN defaults.
                    // Do not set employeeFound=true so the counter fires later.
                    context.getCounter(PayrollReducerCounters.MALFORMED_PAYROLL_VALUES).increment(1);
                    continue;
                }

                // Construct the human-readable full name and extract department.
                fullName      = empFields[0].trim() + " " + empFields[1].trim();
                department    = empFields[2].trim();
                employeeFound = true;

            // ── Branch B: Payroll transaction record ──────────────────────────
            } else if (taggedValue.startsWith(PAYROLL_TAG)) {

                // Strip the "pay~" tag to obtain the raw payload.
                // Expected payload: month,baseSalary,bonus
                String payPayload = taggedValue.substring(PAYROLL_TAG.length());
                String[] payFields = payPayload.split(DELIMITER, -1);

                // Validate field count and emptiness before numeric parsing.
                if (payFields.length != 3
                        || payFields[0].trim().isEmpty()
                        || payFields[1].trim().isEmpty()
                        || payFields[2].trim().isEmpty()) {

                    context.getCounter(PayrollReducerCounters.MALFORMED_PAYROLL_VALUES).increment(1);
                    continue;
                }

                String month     = payFields[0].trim();
                String salaryStr = payFields[1].trim();
                String bonusStr  = payFields[2].trim();

                // Numeric parsing — wrap in try-catch to handle non-integer values
                // such as "N/A", "-", or floating-point strings.
                int baseSalary;
                int bonus;
                try {
                    baseSalary = Integer.parseInt(salaryStr);
                    bonus      = Integer.parseInt(bonusStr);
                } catch (NumberFormatException e) {
                    context.getCounter(PayrollReducerCounters.MALFORMED_PAYROLL_VALUES).increment(1);
                    continue;
                }

                // Guard against negative compensation values (business rule).
                if (baseSalary < 0 || bonus < 0) {
                    context.getCounter(PayrollReducerCounters.MALFORMED_PAYROLL_VALUES).increment(1);
                    continue;
                }

                // All validations passed — create and buffer the parsed record.
                PayrollRecord record = new PayrollRecord(month, baseSalary, bonus);
                payrollRecords.add(record);

                // Update the running maximum totalPay across all pay periods.
                // This single comparison is O(1) and avoids a separate sort pass.
                if (record.totalPay > maxPay) {
                    maxPay = record.totalPay;
                }

            // ── Branch C: Unrecognised tag ────────────────────────────────────
            } else {
                // A value arrived with an unknown prefix — log via counter and skip.
                // This should never happen in a correctly configured job but is
                // handled defensively for robustness.
                context.getCounter(PayrollReducerCounters.UNRECOGNISED_TAG_SKIPPED).increment(1);
            }
        }

        // ═════════════════════════════════════════════════════════════════════
        //  Post-collection checks before emission
        // ═════════════════════════════════════════════════════════════════════

        // Track keys where no employee directory record was present.
        // Output is still produced (using UNKNOWN defaults) so no payroll
        // data is silently discarded.
        if (!employeeFound) {
            context.getCounter(PayrollReducerCounters.MISSING_EMPLOYEE_RECORDS).increment(1);
        }

        // If no valid payroll records were collected for this employee,
        // there is nothing meaningful to emit — skip the key entirely.
        if (payrollRecords.isEmpty()) {
            context.getCounter(PayrollReducerCounters.NO_PAYROLL_RECORDS).increment(1);
            return;
        }

        // ═════════════════════════════════════════════════════════════════════
        //  PASS 2 — Emit one enriched line per buffered payroll record
        // ─────────────────────────────────────────────────────────────────────
        //  maxPay is now final — all records for this employee have been seen.
        //  Each output line carries maxPay so downstream consumers can rank or
        //  filter records without a second MapReduce pass.
        //
        //  Output format (value):
        //    fullName,department,month,totalPay,maxPay
        //
        //  Combined with the key, the full CSV row is:
        //    employeeId,fullName,department,month,totalPay,maxPay
        // ═════════════════════════════════════════════════════════════════════

        String employeeId = key.toString();

        for (PayrollRecord record : payrollRecords) {

            // Reset the shared StringBuilder before building the next line.
            lineBuilder.setLength(0);

            lineBuilder.append(fullName)
                       .append(DELIMITER)
                       .append(department)
                       .append(DELIMITER)
                       .append(record.month)
                       .append(DELIMITER)
                       .append(record.totalPay)
                       .append(DELIMITER)
                       .append(maxPay);

            outputValue.set(lineBuilder.toString());
            context.write(key, outputValue);

            context.getCounter(PayrollReducerCounters.RECORDS_EMITTED).increment(1);
        }
    }

    // ────────────────────────────────────────────────────────────────────────
    //  Lifecycle – cleanup
    // ────────────────────────────────────────────────────────────────────────

    /**
     * Invoked once per Reducer task after all keys have been processed.
     *
     * <p>Placeholder for releasing any resources acquired in {@code setup()}.
     * No external resources are held by this implementation.
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