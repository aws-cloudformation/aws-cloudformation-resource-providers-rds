package software.amazon.rds.common.logging;

import java.util.Collection;
import java.util.Map;

public enum ConcreteTypes {
    String(String.class),
    Integer(Integer.class),
    Double(Double.class),
    Boolean(Boolean.class),
    Collection(Collection.class),
    Map(Map.class);

    private final Class<?> typeClass;

    ConcreteTypes(Class<?> typeClass) {
        this.typeClass = typeClass;
    }

    boolean isInstanceOf(Object obj) {
        return typeClass.isInstance(obj);
    }

}
