package software.amazon.rds.dbinstance.status;

import software.amazon.awssdk.utils.StringUtils;
import software.amazon.rds.common.status.Status;

public enum VPCSecurityGroupStatus implements Status {
    Active("active");

    private final String value;

    VPCSecurityGroupStatus(final String value) {
        this.value = value;
    }

    public static VPCSecurityGroupStatus fromString(final String status) {
        for (VPCSecurityGroupStatus vpcSecurityGroupStatus : VPCSecurityGroupStatus.values()) {
            if (vpcSecurityGroupStatus.equalsString(status)) {
                return vpcSecurityGroupStatus;
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
