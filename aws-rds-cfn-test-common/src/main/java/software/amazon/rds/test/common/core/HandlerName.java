package software.amazon.rds.test.common.core;

import software.amazon.rds.test.common.annotations.ExcludeFromJacocoGeneratedReport;

@ExcludeFromJacocoGeneratedReport
public enum HandlerName {
    LIST("list"),
    READ("read"),
    CREATE("create"),
    UPDATE("update"),
    DELETE("delete");

    private final String name;

    private HandlerName(final String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return this.name;
    }
}
