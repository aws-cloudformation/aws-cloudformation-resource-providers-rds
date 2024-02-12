package software.amazon.rds.common.error;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import lombok.NonNull;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

public class ErrorRuleSet implements Cloneable {

    public static ErrorRuleSet EMPTY_RULE_SET = new ErrorRuleSet(null, Collections.emptyMap(), Collections.emptyMap());

    final Map<ErrorCode, ErrorStatus> errorCodeMap;

    final Map<Class<?>, ErrorStatus> errorClassMap;

    ErrorRuleSet base;

    private ErrorRuleSet(
            final ErrorRuleSet base,
            final Map<Class<?>, ErrorStatus> errorClassMap,
            final Map<ErrorCode, ErrorStatus> errorCodeMap
    ) {
        this.base = base;
        this.errorCodeMap = errorCodeMap;
        this.errorClassMap = errorClassMap;
    }

    private ErrorRuleSet(final Builder builder) {
        this(builder.base, new LinkedHashMap<>(builder.errorClassMap), new LinkedHashMap<>(builder.errorCodeMap));
    }

    public static Builder extend(final ErrorRuleSet base) {
        return new Builder(base);
    }

    @NonNull
    public ErrorStatus handle(final Exception exception) {
        if (errorClassMap.containsKey(exception.getClass())) {
            return errorClassMap.get(exception.getClass()).interpret(exception);
        } else if (exception instanceof AwsServiceException) {
            final AwsServiceException awsServiceException = (AwsServiceException) exception;
            final AwsErrorDetails errorDetails = awsServiceException.awsErrorDetails();
            if (errorDetails != null) {
                final String errorStr = errorDetails.errorCode();
                final ErrorCode errorCode = ErrorCode.fromString(errorStr);
                if (errorCode != null && errorCodeMap.containsKey(errorCode)) {
                    return errorCodeMap.get(errorCode).interpret(exception);
                }
            }
        }
        if (base != null) {
            return base.handle(exception);
        }
        return new UnexpectedErrorStatus(exception);
    }

    @Override
    public ErrorRuleSet clone() {
        return new ErrorRuleSet(
                this.base,
                new LinkedHashMap<>(this.errorClassMap),
                new LinkedHashMap<>(this.errorCodeMap)
        );
    }

    public ErrorRuleSet extendWith(@NonNull final ErrorRuleSet extension) {
        ErrorRuleSet extended = extension.clone();
        ErrorRuleSet ptr = extended;
        while (ptr.base != null && ptr.base != EMPTY_RULE_SET) {
            ptr.base = ptr.base.clone();
            ptr = ptr.base;
        }
        ptr.base = this;

        return extended;
    }

    public static class Builder {
        final ErrorRuleSet base;
        final Map<Class<?>, ErrorStatus> errorClassMap;
        final Map<ErrorCode, ErrorStatus> errorCodeMap;

        protected Builder(@NonNull final ErrorRuleSet base) {
            this.base = base;
            this.errorClassMap = new LinkedHashMap<>();
            this.errorCodeMap = new LinkedHashMap<>();
        }

        public Builder withErrorClasses(final ErrorStatus errorStatus, final Class<?>... errorClasses) {
            for (final Class<?> errorClass : errorClasses) {
                errorClassMap.put(errorClass, errorStatus);
            }
            return this;
        }

        public Builder withErrorCodes(final ErrorStatus errorStatus, final ErrorCode... errorCodes) {
            for (final ErrorCode errorCode : errorCodes) {
                errorCodeMap.put(errorCode, errorStatus);
            }
            return this;
        }

        public ErrorRuleSet build() {
            return new ErrorRuleSet(this);
        }
    }
}
