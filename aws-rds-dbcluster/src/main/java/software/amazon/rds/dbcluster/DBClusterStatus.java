package software.amazon.rds.dbcluster;

public enum DBClusterStatus {
    Available("available"),
    Creating("creating"),
    Deleted("deleted");

    private String value;

    DBClusterStatus(String value) {
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
