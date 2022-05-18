package software.amazon.rds.dbinstance.client;

import lombok.NonNull;

public class UnknownVersionException extends RuntimeException {
    static final long serialVersionUID = 20000012141L;

    public UnknownVersionException(@NonNull final ApiVersion apiVersion) {
        super(String.format("Version '%s' is not registered", apiVersion));
    }
}
