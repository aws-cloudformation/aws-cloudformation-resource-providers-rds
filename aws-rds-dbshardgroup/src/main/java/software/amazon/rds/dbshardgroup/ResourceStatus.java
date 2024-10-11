package software.amazon.rds.dbshardgroup;

import software.amazon.awssdk.utils.StringUtils;

public enum ResourceStatus {
    AVAILABLE("available"),
    CREATING("creating"),
    DELETING("deleting"),
    MODIFYING("modifying");

    private final String value;

    ResourceStatus(final String value) {
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
