package software.amazon.rds.common.logging;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONObject;

import lombok.NonNull;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@lombok.Getter
public class RequestLogger {

    public static final Logger NULL_LOGGER = Function.identity()::apply; 
    public static final RequestLogger NULL_REQUEST_LOGGER = new RequestLogger(NULL_LOGGER, new ResourceHandlerRequest<>(), parameterName -> true);
    
    private Logger logger;
    private CustomerRequestData customerRequestData;
    private ReflectionToJson reflectionToJson;

    public <T> RequestLogger(final Logger logger,
                             final @NonNull ResourceHandlerRequest<T> request,
                             final Predicate<String> acceptParameters) {
        this.logger = Optional.ofNullable(logger).orElse(NULL_LOGGER);
        customerRequestData = new CustomerRequestData(
                request.getStackId(),
                request.getClientRequestToken(),
                request.getAwsAccountId());
        reflectionToJson = new ReflectionToJson(acceptParameters);
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

    public void log(String message) {
        log(message, null, null);
    }

    public void log(String message, Object object) {
        log(message, object, null);
    }

    //Adding customer data to the end for better experience. To make first line in log message meaningful
    public void log(String message, Object object, Collection<String> excludeParameters) {
        try {
            JSONObject jsonLog;
            StringBuilder sb = new StringBuilder();
            String objectLog = StringUtils.EMPTY;
            sb.append(message);
            //Convert object to string
            if (object != null) {
                jsonLog = reflectionToJson.buildJson(object);
                sb.append(jsonLog);
            }
            sb.append(objectLog);
            //Adding customer request data
            sb.append(StringUtils.LF);
            sb.append(customerRequestData);
            logMessage(sb.toString());
        } catch (Throwable throwable) {
            logMessage(throwable.toString());
        }
    }

    private void logMessage(final String message) {
        logger.log(message);
    }
}
