package software.amazon.rds.dbinstance;

public enum StorageType {
    GP2("gp2"),
    GP3("gp3"),
    IO1("io1"),
    MAGNETIC("magnetic");

    private final String value;

    public static StorageType fromString(final String value) {
        for (final StorageType storageType: StorageType.values()) {
            if (storageType.value.equalsIgnoreCase(value)) {
                return storageType;
            }
        }
        return null;
    }

    StorageType(final String value) {
        this.value = value;
    }
}
