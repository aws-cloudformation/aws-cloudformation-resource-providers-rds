package software.amazon.rds.common.error;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public interface ErrorRuleSet {
    ErrorStatus handle(Exception exception);

    default ErrorRuleSet orElse(ErrorRuleSet another) {
        return new OrErrorRuleSet(Arrays.asList(this, another));
    }

    class Builder {
        final Map<Class<?>, ErrorStatus> errorClassMap;
        final Map<ErrorCode, ErrorStatus> errorCodeMap;

        protected Builder() {
            this.errorClassMap = new LinkedHashMap<>();
            this.errorCodeMap = new LinkedHashMap<>();
        }

        public Builder withErrorClasses(final Collection<Class<?>> errorClasses, final ErrorStatus errorStatus) {
            for (final Class<?> errorClass : errorClasses) {
                errorClassMap.put(errorClass, errorStatus);
            }
            return this;
        }

        public Builder withErrorCodes(final Collection<ErrorCode> errorCodes, final ErrorStatus errorStatus) {
            for (final ErrorCode errorCode : errorCodes) {
                errorCodeMap.put(errorCode, errorStatus);
            }
            return this;
        }

        public ErrorRuleSet build() {
            return new PlainErrorRuleSet(this);
        }
    }

    static Builder builder() {
        return new Builder();
    }
}
