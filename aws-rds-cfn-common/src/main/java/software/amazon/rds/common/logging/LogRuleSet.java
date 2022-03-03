package software.amazon.rds.common.logging;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import lombok.AllArgsConstructor;

public interface LogRuleSet {
    void accept(Object object);

    @AllArgsConstructor
    class LogRule<T> {
        public Consumer<T> consumer;

        //This will be checked in accept method.
        @SuppressWarnings("unchecked")
        void applyLogRule(Object object) {
            consumer.accept((T) object);
        }
    }

    class Builder {
        final Map<Class<?>, LogRule<?>> logClassMap;

        protected Builder() {
            this.logClassMap = new LinkedHashMap<>();
        }

        public LogRuleSet.Builder withLogClasses(final Consumer<?> logConsumer, final Class<?>... logClasses) {
            LogRule<?> logRule = new LogRule<>(logConsumer);
            for (final Class<?> logClass : logClasses) {
                logClassMap.put(logClass, logRule);
            }
            return this;
        }

        public LogRuleSet build() {
            return new PlainLogRuleSet(this);
        }
    }

    static LogRuleSet.Builder builder() {
        return new LogRuleSet.Builder();
    }
}
