package software.amazon.rds.common.handler;


import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;


@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode
public class ProbingContext {

    public interface Provider {
        ProbingContext getProbingContext();
    }

    private boolean isProbingEnabled;
    private final Map<String, Integer> probes;

    public ProbingContext() {
        probes = new HashMap<>();
    }

    protected int getProbes(final String sampleName) {
        return this.probes.getOrDefault(sampleName, 0);
    }

    protected int incProbes(final String sampleName) {
        return this.probes.merge(sampleName, 1, Integer::sum);
    }

    protected void flushProbes(final String sampleName) {
        this.probes.remove(sampleName);
    }

    public boolean withProbing(
            final String probeName,
            final int nProbes,
            final Supplier<Boolean> checker
    ) {
        final boolean check = checker.get();
        if (!this.isProbingEnabled()) {
            return check;
        }
        if (!check) {
            this.flushProbes(probeName);
            return false;
        }
        this.incProbes(probeName);
        if (this.getProbes(probeName) >= nProbes) {
            this.flushProbes(probeName);
            return true;
        }
        return false;
    }
}
