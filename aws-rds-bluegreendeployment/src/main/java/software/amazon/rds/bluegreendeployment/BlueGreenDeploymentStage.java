package software.amazon.rds.bluegreendeployment;

public enum BlueGreenDeploymentStage {
    Blue("blue"),
    Green("green");

    private final String value;

    BlueGreenDeploymentStage(final String value) {
        this.value = value;
    }

    public boolean equalsString(final String other) {
        return this.value.equalsIgnoreCase(other);
    }
}
