package software.amazon.rds.common.util;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class TranslatorUtils {

    public static<M> M difference(final M previousModel, final M desiredModel, Supplier<M> instanceSupplier) {
        if (Objects.isNull(previousModel)) {
            return desiredModel;
        }

        try {
            M result = instanceSupplier.get();

            for (final Method setMethod : Arrays.stream(result.getClass().getDeclaredMethods()).filter(m -> m.isAnnotationPresent(JsonProperty.class)).collect(Collectors.toList())) {
                final String setMethodName = setMethod.getName();
                final String getMethodName = setMethodName.replaceFirst("^set", "get");
                final Method getMethod = result.getClass().getMethod(getMethodName);

                if (!Objects.deepEquals(getMethod.invoke(desiredModel), getMethod.invoke(previousModel))) {
                    setMethod.invoke(result, getMethod.invoke(desiredModel));
                }
            }

            return result;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
