package software.amazon.rds.dbparametergroup;

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
