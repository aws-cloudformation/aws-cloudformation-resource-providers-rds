package software.amazon.rds.dbclusterparametergroup;

public enum ParameterType {
    Static("static"),
    Dynamic("dynamic");

    private final String value;

    ParameterType(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
