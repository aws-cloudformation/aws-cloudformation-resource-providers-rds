package software.amazon.rds.dbinstance.status;

import software.amazon.awssdk.utils.StringUtils;
import software.amazon.rds.common.status.Status;

public enum ReadReplicaStatus implements Status {
    Replicating("replicating");

    private final String value;

    ReadReplicaStatus(final String value) {
        this.value = value;
    }

    public static ReadReplicaStatus fromString(final String status) {
        for (final ReadReplicaStatus readReplicaStatus : ReadReplicaStatus.values()) {
            if (readReplicaStatus.equalsString(status)) {
                return readReplicaStatus;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return this.value;
    }

    @Override
    public boolean equalsString(final String other) {
        return StringUtils.equals(this.value, other);
    }
}
