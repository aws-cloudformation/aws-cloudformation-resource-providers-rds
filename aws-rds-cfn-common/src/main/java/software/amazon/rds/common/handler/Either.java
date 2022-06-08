package software.amazon.rds.common.handler;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import java.util.Optional;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Either<T, U> {
    @Getter
    private final Optional<T> left;

    @Getter
    private final Optional<U> right;

    public static <T,U> Either<T,U> left(@NonNull T value) {
        return new Either<>(Optional.of(value), Optional.empty());
    }
    public static <T,U> Either<T,U> right(@NonNull U value) {
        return new Either<>(Optional.empty(), Optional.of(value));
    }
}
