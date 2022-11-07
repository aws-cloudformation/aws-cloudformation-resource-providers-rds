package software.amazon.rds.customdbengineversion;

public enum CustomDBEngineVersionStatus {
    Available("available", StatusOption.Stable),
    Creating("creating", StatusOption.Transient),
    Deleting("deleting", StatusOption.Transient),
    Deprecated("deprecated", StatusOption.Terminal),
    Failed("failed", StatusOption.Terminal),
    Inactive("inactive", StatusOption.Stable),
    InactiveExceptRestore("inactive-except-restore", StatusOption.Stable),
    IncompatibleImageConfiguration("incompatible-image-configuration", StatusOption.Terminal),
    PendingValidation("pending-validation", StatusOption.Transient),
    Validating("validating", StatusOption.Transient);

    private final String value;
     private final StatusOption statusOption;


    CustomDBEngineVersionStatus(final String value, final StatusOption statusOption) {
        this.value = value;
        this.statusOption = statusOption;
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

    public boolean equalsString(final String status) {
        return this.value.equals(status);
    }

    public boolean isTerminal() {
        return StatusOption.Terminal.equals(statusOption);
    }

    public boolean isStable() {
        return StatusOption.Stable.equals(statusOption);
    }
}
