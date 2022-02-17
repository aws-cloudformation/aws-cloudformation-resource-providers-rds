package software.amazon.rds.common.logging;

import java.util.LinkedHashMap;
import java.util.Map;

public class PlainLogRuleSet implements LogRuleSet {
    Map<Class<?>, LogRule<?>> logClassMap;

    public PlainLogRuleSet(final Builder builder) {
        this.logClassMap = new LinkedHashMap<>(builder.logClassMap);
    }

    @Override
    public void accept(final Object object) {
        for (Class<?> logClass : logClassMap.keySet()) {
            if (object != null && logClass.isInstance(object)) {
                logClassMap.get(logClass).applyLogRule(object);
                break;
            }
        }
    }

}
