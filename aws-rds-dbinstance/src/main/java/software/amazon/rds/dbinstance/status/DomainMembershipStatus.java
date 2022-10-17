package software.amazon.rds.dbinstance.status;

import software.amazon.awssdk.utils.StringUtils;
import software.amazon.rds.common.status.Status;

public enum DomainMembershipStatus implements Status {
    Joined("joined"),
    KerberosEnabled("kerberos-enabled");

    private final String value;

    DomainMembershipStatus(final String value) {
        this.value = value;
    }

    public static DomainMembershipStatus fromString(final String status) {
        for (final DomainMembershipStatus domainMembershipStatus : DomainMembershipStatus.values()) {
            if (domainMembershipStatus.equalsString(status)) {
                return domainMembershipStatus;
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
