package software.amazon.rds.common.logging;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import lombok.NonNull;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.printer.JsonPrinter;

@lombok.Getter
@lombok.Setter
public class RequestLogger {

    public static final String THROWABLE_MARKER = "Throwable";
    public static final String CONTENT = "Content";
    public static final String STACK_ID = "StackId";
    public static final String AWS_ACCOUNT_ID = "AwsAccountId";
    public static final String CLIENT_REQUEST_TOKEN = "ClientRequestToken";
    private final Logger logger;
    private final Map<String, String> requestDataMap;
    private final JsonPrinter jsonPrinter;

    private final LogRuleSet DEFAULT_LOG_RULE_SET = LogRuleSet.builder()
            .withLogClasses((Throwable t) -> log(t),
                    Throwable.class)
            .withLogClasses((Iterable<?> it) -> it.forEach(r -> log(r)),
                    Iterable.class)
            .withLogClasses((ResponseBytes<?> rb) -> log(rb.response().getClass().getSimpleName(), rb.response()),
                    ResponseBytes.class)
            .withLogClasses((ResponseInputStream<?> is) -> log(is.response().getClass().getSimpleName(), is.response()),
                    ResponseInputStream.class)
            .withLogClasses((String s) -> log(s, s),
                    String.class)
            .withLogClasses((Object r) -> log(r.getClass().getSimpleName(), r),
                    AwsRequest.class,
                    AwsResponse.class,
                    CompletableFuture.class,
                    Object.class)
            .build();

    public <T> RequestLogger(final Logger logger,
                             final @NonNull ResourceHandlerRequest<T> request,
                             final JsonPrinter jsonPrinter) {
        this.logger = logger;
        this.jsonPrinter = jsonPrinter;
        this.requestDataMap = new HashMap<>();
        requestDataMap.put(STACK_ID, request.getStackId());
        requestDataMap.put(AWS_ACCOUNT_ID, request.getAwsAccountId());
        requestDataMap.put(CLIENT_REQUEST_TOKEN, request.getClientRequestToken());
    }

    public static <M, C> ProgressEvent<M, C> handleRequest(final Logger logger,
                                                           final @NonNull ResourceHandlerRequest<M> request,
                                                           final JsonPrinter jsonPrinter,
                                                           final Function<RequestLogger, ProgressEvent<M, C>> requestHandler) {
        RequestLogger requestLogger = new RequestLogger(logger, request, jsonPrinter);
        requestLogger.log("HandlerRequest", request);
        ProgressEvent<M, C> progressEvent = null;
        try {
            progressEvent = requestHandler.apply(requestLogger);
            requestLogger.log("HandlerResponse", progressEvent);
        } catch (Throwable throwable) {
            requestLogger.logAndThrow(throwable);
        }
        return progressEvent;
    }

    public void log(Throwable throwable) {
        try {
            LogMessage message = JsonLogMessage.newLogMessage(jsonPrinter);
            message.append(CONTENT, throwable.getClass().getCanonicalName());
            message.append(throwable);
            message.append(requestDataMap);
            logMessage(message);
        } catch (Throwable caughtThrowable) {
            logMessage(caughtThrowable);
        }
    }

    public void logAndThrow(Throwable throwable) {
        log(throwable);
        ExceptionUtils.rethrow(throwable);
    }

    public void log(Object object) {
        try {
            DEFAULT_LOG_RULE_SET.accept(object);
        } catch (Throwable throwable) {
            logMessage(throwable);
        }
    }

    public void log(String message, Object object) {
        log(message, object, null);
    }

    public void log(String msg, Object object, Map<String, String> additionalFields) {
        try {
            LogMessage message = JsonLogMessage.newLogMessage(jsonPrinter);
            message.append(CONTENT, msg);
            message.append(object);
            message.append(additionalFields);
            message.append(requestDataMap);
            logMessage(message);
        } catch (Throwable throwable) {
            logMessage(throwable);
            logMessage(ObjectUtils.defaultIfNull(object, StringUtils.EMPTY).toString());
        }
    }

    private void logMessage(final LogMessage message) {
        logMessage(message.toString());
    }

    private void logMessage(final String message) {
        if (logger != null) {
            logger.log(message);
        }
    }

    private void logMessage(final Throwable throwable) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(requestDataMap);
        stringBuilder.append(StringUtils.LF);
        stringBuilder.append(ExceptionUtils.getStackTrace(throwable));
        logMessage(stringBuilder.toString());
    }
}
