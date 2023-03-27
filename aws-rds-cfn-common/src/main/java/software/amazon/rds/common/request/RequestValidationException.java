package software.amazon.rds.common.request;

@SuppressWarnings("serial")
public class RequestValidationException extends Exception {
    public RequestValidationException(final String message) {
        super(message);
    }
}
