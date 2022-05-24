package software.amazon.rds.common.error;

import java.util.LinkedHashMap;
import java.util.Map;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;

public class PlainErrorRuleSet implements ErrorRuleSet {
    final Map<Class<?>, ErrorStatus> errorClassMap;
    final Map<ErrorCode, ErrorStatus> errorCodeMap;

    protected PlainErrorRuleSet(final Builder builder) {
        this.errorCodeMap = new LinkedHashMap<>(builder.errorCodeMap);
        this.errorClassMap = new LinkedHashMap<>(builder.errorClassMap);
    }

    public ErrorStatus handle(final Exception exception) {
        ErrorStatus errorStatus = new UnexpectedErrorStatus(exception);

        if (errorClassMap.containsKey(exception.getClass())) {
            errorStatus = errorClassMap.get(exception.getClass());
        } else if (exception instanceof AwsServiceException) {
            final AwsServiceException awsServiceException = (AwsServiceException) exception;
            final AwsErrorDetails errorDetails = awsServiceException.awsErrorDetails();
            if (errorDetails != null) {
                final String errorStr = errorDetails.errorCode();
                final ErrorCode errorCode = ErrorCode.fromString(errorStr);
                if (errorCode != null && errorCodeMap.containsKey(errorCode)) {
                    errorStatus = errorCodeMap.get(errorCode);
                }
            }
        }
        return errorStatus.interpret(exception);
    }
}
