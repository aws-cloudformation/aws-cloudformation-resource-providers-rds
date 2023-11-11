package software.amazon.rds.bluegreendeployment;

public enum BlueGreenDeploymentTaskStatus {
    Completed("completed");

    private final String value;

    BlueGreenDeploymentTaskStatus(final String value) {
        this.value = value;
    }

    public boolean equalsString(final String other) {
        return this.value.equalsIgnoreCase(other);
    }
}
