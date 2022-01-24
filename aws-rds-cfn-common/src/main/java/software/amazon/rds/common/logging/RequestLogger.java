package software.amazon.rds.common.logging;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.exception.ExceptionUtils;

import lombok.NonNull;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.logging.CustomerRequestData;

@lombok.Getter
@lombok.Setter
public class RequestLogger {

    private Logger logger;
    private CustomerRequestData customerRequestData;

    public <T> RequestLogger(Logger logger, @NonNull ResourceHandlerRequest<T> request) {
        this.logger = logger;
        customerRequestData = new CustomerRequestData(
                request.getClientRequestToken(),
                request.getAwsAccountId(),
                request.getStackId());
    }

    public void logAndThrow(Throwable throwable) {
        try {
            StringBuilder sb = new StringBuilder();
            sb.append(ExceptionUtils.getStackTrace(throwable));
            sb.append(customerRequestData);
            logMessage(sb.toString());
            //Logging throwable as object to include its fields i.e: awsErrorDetails, awsErrorCode...
            log("Throwable Object: ", throwable);
        } catch (Throwable caughtThrowable) {
            logMessage(caughtThrowable.toString());
        }
        ExceptionUtils.rethrow(throwable);
    }

    public void log(String message, Object object) {
        log(message, object, null);
    }

    //Adding customer data to the end for better experience. To make first line in log message meaningful
    public void log(String message, Object object, Collection<String> excludeParameters) {
        try {
            ReflectionToStringBuilder.setDefaultStyle(ToStringStyle.MULTI_LINE_STYLE);
            String objectLog = ReflectionToStringBuilder.toStringExclude(
                    Optional.ofNullable(object).orElse(ObjectUtils.NULL),
                    Optional.ofNullable(excludeParameters).orElse(Collections.emptyList()));
            StringBuilder sb = new StringBuilder();
            sb.append(message);
            sb.append(objectLog);
            sb.append(customerRequestData);
            logMessage(sb.toString());
        } catch (Throwable throwable) {
            logMessage(throwable.toString());
        }
    }

    private void logMessage(final String message) {
        if (logger != null){
            logger.log(message);
        }
    }
}
