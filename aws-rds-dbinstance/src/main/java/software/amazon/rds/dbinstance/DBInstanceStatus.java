package software.amazon.rds.dbinstance;

import software.amazon.awssdk.utils.StringUtils;

public enum DBInstanceStatus {
    Available("available"),
    Creating("creating"),
    Deleting("deleting"),
    Failed("failed");

    private String value;

    DBInstanceStatus(final String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public boolean equalsString(final String other) {
        return StringUtils.equals(value, other);
    }
}
