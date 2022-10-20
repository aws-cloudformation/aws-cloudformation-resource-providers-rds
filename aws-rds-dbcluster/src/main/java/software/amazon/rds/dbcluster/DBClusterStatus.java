package software.amazon.rds.dbcluster;

import software.amazon.rds.common.status.TerminableStatus;

public enum DBClusterStatus implements TerminableStatus {
    Available("available"),
    Creating("creating"),
    Deleted("deleted"),
    InaccessibleEncryptionCredentials("inaccessible-encryption-credentials", true),
    IncompatibleNetwork("incompatible-network", true),
    IncompatibleParameters("incompatible-parameters", true),
    IncompatibleRestore("incompatible-restore", true);

    private final String value;
    private final boolean isTerminal;

    DBClusterStatus(final String value) {
        this(value, false);
    }
    DBClusterStatus(final String value,final  boolean isTerminal) {
        this.value = value;
        this.isTerminal = isTerminal;
    }

    public static DBClusterStatus fromString(final String source) {
        for (final DBClusterStatus status: DBClusterStatus.values()) {
            if (status.equalsString(source)) {
                return status;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return value;
    }

    public boolean equalsString(final String status) {
        return this.value.equals(status);
    }

    @Override
    public boolean isTerminal() {
        return this.isTerminal;
    }
}
