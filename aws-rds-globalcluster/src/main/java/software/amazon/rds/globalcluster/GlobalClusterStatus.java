package software.amazon.rds.globalcluster;

public enum GlobalClusterStatus {
    Available("available");

    private String value;

    GlobalClusterStatus(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    public boolean equalsString(final String status) {
        return this.value.equals(status);
    }
}
