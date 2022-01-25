package software.amazon.rds.common.logging;

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

    public static final String MESSAGE_MARKER = "Message";
    public static final String THROWABLE_MARKER = "Throwable";
    public static final String REQUEST_DATA_MARKER = "RequestData";
    private final Logger logger;
    private final JSONObject requestData;
    private final JsonPrinter jsonPrinter;

    public <T> RequestLogger(final Logger logger,
                             final @NonNull ResourceHandlerRequest<T> request,
                             final JsonPrinter jsonPrinter) {
        this.logger = logger;
        this.jsonPrinter = jsonPrinter;
        requestData = new RequestData(request.getStackId(), request.getAwsAccountId(), request.getClientRequestToken())
                .toJson();
    }

    public static <M, C> ProgressEvent<M, C> handleRequest(final Logger logger,
                                                           final @NonNull ResourceHandlerRequest<M> request,
                                                           final JsonPrinter jsonPrinter,
                                                           final Function<RequestLogger, ProgressEvent<M, C>> requestHandler) {
        RequestLogger requestLogger = new RequestLogger(logger, request, jsonPrinter);
        requestLogger.log("Request", request);
        ProgressEvent<M, C> progressEvent = null;
        try {
            progressEvent = requestHandler.apply(requestLogger);
            requestLogger.log("Response", progressEvent);
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
            jsonObject.put(REQUEST_DATA_MARKER, requestData);
            logMessage(jsonObject.toString());
        } catch (Throwable caughtThrowable) {
            logMessage(throwable);
        }
    }

    public void log(String message) {
        log(MESSAGE_MARKER, message);
    }

    public void log(String marker, Object object) {
        try {
            String objectAsString = jsonPrinter.print(ObjectUtils.defaultIfNull(object, StringUtils.EMPTY));
            JSONObject jsonLog = new JSONObject();
            jsonLog.put(marker, new JSONObject(objectAsString));
            jsonLog.put(REQUEST_DATA_MARKER, requestData);
            logMessage(jsonLog.toString());
        } catch (Throwable throwable) {
            logMessage(throwable);
        }
    }

    private void logMessage(final String message) {
        if (logger != null) {
            logger.log(message);
        }
    }

    private void logMessage(final Throwable throwable) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(requestData);
        stringBuilder.append(StringUtils.LF);
        stringBuilder.append(ExceptionUtils.getStackTrace(throwable));
        logMessage(stringBuilder.toString());
    }
}
