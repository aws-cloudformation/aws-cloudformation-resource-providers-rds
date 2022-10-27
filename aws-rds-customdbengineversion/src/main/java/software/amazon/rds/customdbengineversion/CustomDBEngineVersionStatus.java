package software.amazon.rds.customdbengineversion;

import software.amazon.rds.common.status.StableStatus;
import software.amazon.rds.common.status.TerminableStatus;

public enum CustomDBEngineVersionStatus implements TerminableStatus, StableStatus {
    Available("available", false, true),
    Creating("creating"),
    Deleting("deleting"),
    Failed("failed", true, false),
    Inactive("inactive", false, true),
    InactiveExceptRestore("inactive-except-restore", false, true),
    IncompatibleImageConfiguration("incompatible-image-configuration", true, false),
    PendingValidation("pending-validation"),
    Validating("validating");

    private final String value;
    private final boolean isTerminal;
    private final boolean isStable;

    CustomDBEngineVersionStatus(final String value) {
        this(value, false, false);
    }

    CustomDBEngineVersionStatus(final String value, final boolean isTerminal, final boolean isStable) {
        this.value = value;
        this.isTerminal = isTerminal;
        this.isStable = isStable;
    }

    public static CustomDBEngineVersionStatus fromString(final String source) {
        for (final CustomDBEngineVersionStatus status : CustomDBEngineVersionStatus.values()) {
            if (status.equalsString(source)) {
                return status;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public boolean equalsString(final String status) {
        return this.value.equals(status);
    }

    @Override
    public boolean isTerminal() {
        return this.isTerminal;
    }


    @Override
    public boolean isStable() {
        return this.isStable;
    }
}
