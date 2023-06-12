package software.amazon.rds.common.util;

import software.amazon.rds.common.annotations.ExcludeFromJacocoGeneratedReport;

@ExcludeFromJacocoGeneratedReport
public class DriftDetectorRuntimeException extends RuntimeException {
    static final long serialVersionUID = -7034897190745766939L;

    public DriftDetectorRuntimeException(final String message) {
        super(message);
    }

    public DriftDetectorRuntimeException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public DriftDetectorRuntimeException(final Throwable cause) {
        super(cause);
    }
}
