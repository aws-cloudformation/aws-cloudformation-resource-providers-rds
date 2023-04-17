package software.amazon.rds.dbinstance.status;

import software.amazon.awssdk.utils.StringUtils;
import software.amazon.rds.common.status.TerminableStatus;

public enum DomainMembershipStatus implements TerminableStatus {
    Joined("joined"),
    KerberosEnabled("kerberos-enabled"),
    Failed("failed", true);

    private final String value;
    private final Boolean isTerminal;

    DomainMembershipStatus(final String value) {
        this(value, false);
    }
    DomainMembershipStatus(final String value, final boolean isTerminal) {
        this.value = value;
        this.isTerminal = isTerminal;
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

    @Override
    public boolean isTerminal() {
        return isTerminal;
    }
}
