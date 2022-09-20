package software.amazon.rds.common.handler;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ProbingContextTest {

    final static String PROBE_NAME = "probe";
    final static String PROBE_NAME_ALTER = "probe-alter";

    @Test
    public void incProbesTest() {
        ProbingContext context = new ProbingContext();
        Assertions.assertEquals(0, context.getProbes(PROBE_NAME));
        context.incProbes(PROBE_NAME);
        Assertions.assertEquals(1, context.getProbes(PROBE_NAME));
        context.incProbes(PROBE_NAME_ALTER);
        Assertions.assertEquals(1, context.getProbes(PROBE_NAME));
        Assertions.assertEquals(1, context.getProbes(PROBE_NAME_ALTER));

        context.incProbes(PROBE_NAME);
        Assertions.assertEquals(2, context.getProbes(PROBE_NAME));
    }

    @Test
    public void flushProbesTest() {
        ProbingContext context = new ProbingContext();
        context.incProbes(PROBE_NAME);
        context.incProbes(PROBE_NAME);
        context.incProbes(PROBE_NAME_ALTER);
        Assertions.assertEquals(2, context.getProbes(PROBE_NAME));
        Assertions.assertEquals(1, context.getProbes(PROBE_NAME_ALTER));

        context.flushProbes(PROBE_NAME);
        Assertions.assertEquals(0, context.getProbes(PROBE_NAME));
        Assertions.assertEquals(1, context.getProbes(PROBE_NAME_ALTER));
    }
}
