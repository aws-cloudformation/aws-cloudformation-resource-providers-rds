package software.amazon.rds.common.logging;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.json.JSONObject;

import lombok.NonNull;
import software.amazon.cloudformation.proxy.Logger;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.printer.JsonPrinter;

@lombok.Getter
@lombok.Setter
public class RequestLogger {

    public static final String MESSAGE_EVENT = "Message";
    public static final String THROWABLE_MARKER = "Throwable";
    public static final String EVENT = "Event";
    private final Logger logger;
    private final Map<String, String> requestDataMap;
    private final JsonPrinter jsonPrinter;

    public <T> RequestLogger(final Logger logger,
                             final @NonNull ResourceHandlerRequest<T> request,
                             final JsonPrinter jsonPrinter) {
        this.logger = logger;
        this.jsonPrinter = jsonPrinter;
        requestDataMap = new RequestData(request).getRequestDataMap();
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

    public void logAndThrow(Throwable throwable) {
        log(throwable);
        ExceptionUtils.rethrow(throwable);
    }

    public void log(Throwable throwable) {
        try {
            JSONObject jsonObject = new JSONObject(jsonPrinter.print(throwable));
            addMapToJson(jsonObject, requestDataMap);
            logMessage(jsonObject.toString());
        } catch (Throwable caughtThrowable) {
            logMessage(throwable);
        }
    }

    public void log(String message) {
        log(MESSAGE_EVENT, message);
    }

    public void log(String event, Object object) {
        log(event, object, null);
    }

    public void log(String event, Object object, Map<String, String> additionalFields) {
        try {
            String objectAsString = jsonPrinter.print(ObjectUtils.defaultIfNull(object, StringUtils.EMPTY));
            final JSONObject jsonLog = new JSONObject(objectAsString);
            jsonLog.put(EVENT, event);
            addMapToJson(jsonLog, additionalFields);
            addMapToJson(jsonLog, requestDataMap);
            logMessage(jsonLog.toString());
        } catch (Throwable throwable) {
            logMessage(throwable);
        }
    }

    private void addMapToJson(final JSONObject jsonObject, final Map<String, String> map) {
        Optional.ofNullable(map).orElse(Collections.emptyMap())
                .entrySet().stream().forEach(entry -> jsonObject.put(entry.getKey(), entry.getValue()));
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
