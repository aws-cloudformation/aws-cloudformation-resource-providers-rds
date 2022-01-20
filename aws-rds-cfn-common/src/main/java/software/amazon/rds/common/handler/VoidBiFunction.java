package software.amazon.rds.common.handler;

@FunctionalInterface
public interface VoidBiFunction<T, U> {
    void apply(T t, U u);
}
