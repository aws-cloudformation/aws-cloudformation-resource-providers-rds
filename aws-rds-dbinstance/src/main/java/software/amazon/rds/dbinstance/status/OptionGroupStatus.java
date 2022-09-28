package software.amazon.rds.dbinstance.status;

import software.amazon.awssdk.utils.StringUtils;

public enum OptionGroupStatus implements TerminableStatus {
    InSync("in-sync"),
    Failed("failed", true);

    private final String value;
    private final boolean terminal;

    OptionGroupStatus(final String value) {
        this(value, false);
    }

    OptionGroupStatus(final String value, final boolean terminal) {
        this.value = value;
        this.terminal = terminal;
    }

    public static OptionGroupStatus fromString(final String status) {
        for (final OptionGroupStatus optionGroupStatus : OptionGroupStatus.values()) {
            if (optionGroupStatus.equalsString(status)) {
                return optionGroupStatus;
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
        return this.terminal;
    }
}
