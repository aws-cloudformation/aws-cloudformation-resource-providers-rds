package software.amazon.rds.dbinstance.status;

import software.amazon.awssdk.utils.StringUtils;
import software.amazon.rds.common.status.Status;

public enum DBParameterGroupStatus implements Status {
    Applying("applying"),
    InSync("in-sync"),
    PendingReboot("pending-reboot");

    private final String value;

    DBParameterGroupStatus(final String value) {
        this.value = value;
    }

    public static DBParameterGroupStatus fromString(final String status) {
        for (DBParameterGroupStatus dbParameterGroupStatus : DBParameterGroupStatus.values()) {
            if (dbParameterGroupStatus.equalsString(status)) {
                return dbParameterGroupStatus;
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
