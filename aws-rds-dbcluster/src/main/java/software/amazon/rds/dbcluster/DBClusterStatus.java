package software.amazon.rds.dbcluster;

public class DBClusterStatus {
    public enum Status {
        Available("available"),
        Creating("creating"),
        Deleted("deleted"),
        Failed("failed");

        private String value;

        Status(String value) {
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
}
