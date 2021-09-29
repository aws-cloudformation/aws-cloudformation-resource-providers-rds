package software.amazon.rds.dbinstance.error;

import java.util.LinkedHashMap;
import java.util.Map;

import software.amazon.awssdk.awscore.exception.AwsServiceException;

public class PlainErrorRuleSet implements ErrorRuleSet {
    final Map<Class<?>, ErrorStatus> errorClassMap;
    final Map<ErrorCode, ErrorStatus> errorCodeMap;

    protected PlainErrorRuleSet(final Builder builder) {
        this.errorCodeMap = new LinkedHashMap<>(builder.errorCodeMap);
        this.errorClassMap = new LinkedHashMap<>(builder.errorClassMap);
    }

    public ErrorStatus handle(final Exception exception) {
        if (errorClassMap.containsKey(exception.getClass())) {
            return errorClassMap.get(exception.getClass());
        }
        if (exception instanceof AwsServiceException) {
            final AwsServiceException awsServiceException = (AwsServiceException) exception;
            final String errorStr = awsServiceException.awsErrorDetails().errorCode();
            final ErrorCode errorCode = ErrorCode.fromString(errorStr);
            if (errorCode != null && errorCodeMap.containsKey(errorCode)) {
                return errorCodeMap.get(errorCode);
            }
        }
        return new UnexpectedErrorStatus(exception);
    }
}
