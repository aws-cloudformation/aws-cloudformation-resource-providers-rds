package software.amazon.rds.customdbengineversion;


public enum StatusOption {
    Stable("stable"),
    Transient("transient"),
    Terminal("terminal");

    private final String value;

    StatusOption(final String value) {
        this.value = value;
    }
}
