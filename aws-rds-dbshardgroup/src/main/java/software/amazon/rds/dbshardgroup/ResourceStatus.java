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

    public static ResourceStatus fromString(final String status) {
        for (final ResourceStatus ResourceStatus : ResourceStatus.values()) {
            if (ResourceStatus.equalsString(status)) {
                return ResourceStatus;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return value;
    }

    public boolean equalsString(final String other) {
        return StringUtils.equals(value, other);
    }
}
