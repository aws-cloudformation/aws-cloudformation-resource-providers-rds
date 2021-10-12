package software.amazon.rds.common.error;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class OrErrorRuleSet implements ErrorRuleSet {
    final List<ErrorRuleSet> ruleSets;

    protected OrErrorRuleSet(Collection<ErrorRuleSet> ruleSets) {
        this.ruleSets = new ArrayList<>(ruleSets);
    }

    public ErrorStatus handle(final Exception exception) {
        for (final ErrorRuleSet ruleSet : ruleSets) {
            final ErrorStatus errorStatus = ruleSet.handle(exception);
            if (!(errorStatus instanceof UnexpectedErrorStatus)) {
                return errorStatus;
            }
        }
        return new UnexpectedErrorStatus(exception);
    }
}
