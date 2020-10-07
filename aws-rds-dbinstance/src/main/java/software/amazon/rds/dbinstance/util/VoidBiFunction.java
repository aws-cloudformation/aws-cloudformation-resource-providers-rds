package software.amazon.rds.dbinstance.util;

@FunctionalInterface
public interface VoidBiFunction<T, U> {
    void apply(T t, U u);
}
