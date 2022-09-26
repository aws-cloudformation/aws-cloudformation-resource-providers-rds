package software.amazon.rds.common.handler;

import java.util.function.Supplier;

public final class Probing {
    public static boolean withProbing(
            final ProbingContext context,
            final String probeName,
            final int nProbes,
            final Supplier<Boolean> checker
    ) {
        final boolean check = checker.get();
        if (!context.isProbingEnabled()) {
            return check;
        }
        if (!check) {
            context.flushProbes(probeName);
            return false;
        }
        context.incProbes(probeName);
        if (context.getProbes(probeName) >= nProbes) {
            context.flushProbes(probeName);
            return true;
        }
        return false;
    }
}
