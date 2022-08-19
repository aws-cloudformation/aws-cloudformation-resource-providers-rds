package software.amazon.rds.dbinstance.request;

@SuppressWarnings("serial")
public class RequestValidationException extends Exception {
    public RequestValidationException(final String message) {
        super(message);
    }
}
