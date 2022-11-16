package software.amazon.rds.dbcluster;

public enum EngineMode {
    Serverless("serverless"),
    Provisioned("provisioned"),
    ParallelQuery("parallelquery"),
    MultiMaster("multimaster");

    private final String value;

    EngineMode(final String value) {
        this.value = value;
    }

    public boolean equalsString(final String another) {
        return value.equalsIgnoreCase(another);
    }

    @Override
    public String toString() {
        return value;
    }

    public static EngineMode fromString(final String value) {
        for (final EngineMode engineMode : EngineMode.values()) {
            if (engineMode.value.equalsIgnoreCase(value)) {
                return engineMode;
            }
        }
        return null;
    }
}
