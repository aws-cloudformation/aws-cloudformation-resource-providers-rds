package software.amazon.rds.bluegreendeployment;

public enum BlueGreenDeploymentStatus {
    Available("available"),
    SwitchoverCompleted("switchover_completed");

    private final String value;

    BlueGreenDeploymentStatus(final String value) {
        this.value = value;
    }

    public boolean equalsString(final String value) {
        return this.value.equalsIgnoreCase(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
