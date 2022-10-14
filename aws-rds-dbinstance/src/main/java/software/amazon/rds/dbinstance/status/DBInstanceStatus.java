package software.amazon.rds.dbinstance.status;

import software.amazon.awssdk.utils.StringUtils;
import software.amazon.rds.common.status.TerminableStatus;

public enum DBInstanceStatus implements TerminableStatus {
    Available("available"),
    Creating("creating"),
    Deleting("deleting"),
    Failed("failed", true),
    InaccessibleEncryptionCredentials("inaccessible-encryption-credentials", true),
    IncompatibleNetwork("incompatible-network", true),
    IncompatibleParameters("incompatible-parameters", true),
    IncompatibleRestore("incompatible-restore", true),
    StorageFull("storage-full");

    private final String value;
    private final boolean terminal;

    DBInstanceStatus(final String value) {
        this(value, false);
    }

    DBInstanceStatus(final String value, final boolean terminal) {
        this.value = value;
        this.terminal = terminal;
    }

    public static DBInstanceStatus fromString(final String status) {
        for (final DBInstanceStatus dbInstanceStatus : DBInstanceStatus.values()) {
            if (dbInstanceStatus.equalsString(status)) {
                return dbInstanceStatus;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equalsString(final String other) {
        return StringUtils.equals(value, other);
    }

    @Override
    public boolean isTerminal() {
        return this.terminal;
    }
}
