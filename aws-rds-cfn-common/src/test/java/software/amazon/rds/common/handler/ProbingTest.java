package software.amazon.rds.common.handler;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProbingTest {

    final static String PROBE_NAME = "probe";
    final static int PROBE_COUNT = 3;

    @Test
    public void withProbing_happyPath() {
        ProbingContext context = new ProbingContext();
        context.setProbingEnabled(true);

        Assertions.assertFalse(Probing.withProbing(context, PROBE_NAME, PROBE_COUNT, () -> true));
        Assertions.assertFalse(Probing.withProbing(context, PROBE_NAME, PROBE_COUNT, () -> true));
        Assertions.assertTrue(Probing.withProbing(context, PROBE_NAME, PROBE_COUNT, () -> true));
    }

    @Test
    public void withProbing_resetAfterTwoSuccesses() {
        ProbingContext context = new ProbingContext();
        context.setProbingEnabled(true);

        Assertions.assertFalse(Probing.withProbing(context, PROBE_NAME, PROBE_COUNT, () -> true));
        Assertions.assertFalse(Probing.withProbing(context, PROBE_NAME, PROBE_COUNT, () -> true));
        Assertions.assertFalse(Probing.withProbing(context, PROBE_NAME, PROBE_COUNT, () -> false));
        Assertions.assertFalse(Probing.withProbing(context, PROBE_NAME, PROBE_COUNT, () -> true));
        Assertions.assertFalse(Probing.withProbing(context, PROBE_NAME, PROBE_COUNT, () -> true));
        Assertions.assertTrue(Probing.withProbing(context, PROBE_NAME, PROBE_COUNT, () -> true));
    }
}
