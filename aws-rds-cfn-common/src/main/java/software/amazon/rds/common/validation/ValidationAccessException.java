package software.amazon.rds.common.validation;

@SuppressWarnings("serial")
public class ValidationAccessException extends Exception {
    public ValidationAccessException(final String message) { super(message); }
}
