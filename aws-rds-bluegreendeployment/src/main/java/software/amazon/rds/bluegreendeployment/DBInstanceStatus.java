package software.amazon.rds.bluegreendeployment;

public enum DBInstanceStatus {
    Available("available");

    private final String value;

    DBInstanceStatus(final String value) {
        this.value = value;
    }

    public boolean equalsString(final String other) {
        return this.value.equalsIgnoreCase(other);
    }
}
