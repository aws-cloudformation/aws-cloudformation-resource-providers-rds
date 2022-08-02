package software.amazon.rds.dbinstance;

import software.amazon.awssdk.utils.StringUtils;

public enum DBInstanceStatus {
    Available("available"),
    Creating("creating"),
    Deleting("deleting"),
    Failed("failed", true),
    IncompatibleRestore("incompatible-restore", true),
    IncompatibleNetwork("incompatible-network", true),
    IncompatibleParameters("incompatible-parameters", true),
    InaccessibleEncryptionCredentials("inaccessible-encryption-credentials", true);

    private final String value;
    private final boolean terminal;

    DBInstanceStatus(final String value) {
        this(value, false);
    }

    DBInstanceStatus(final String value, final boolean terminal) {
        this.value = value;
        this.terminal = terminal;
    }

    @Override
    public String toString() {
        return value;
    }

    public static DBInstanceStatus fromString(final String status) {
        try {
            return DBInstanceStatus.valueOf(status);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public boolean equalsString(final String other) {
        return StringUtils.equals(value, other);
    }

    public boolean isTerminal() {
        return this.terminal;
    }
}
