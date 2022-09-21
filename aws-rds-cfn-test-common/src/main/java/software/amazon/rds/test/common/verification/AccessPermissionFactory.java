package software.amazon.rds.test.common.verification;

import java.lang.reflect.Method;

import org.mockito.invocation.Invocation;

import lombok.NonNull;
import software.amazon.rds.test.common.core.ServiceProvider;

public class AccessPermissionFactory {

    public static AccessPermission fromString(final String s) {
        final String[] chunks = s.split(":", 2);
        return new AccessPermission(ServiceProvider.fromString(chunks[0]), chunks[1]);
    }

    public static AccessPermission fromInvocation(final Invocation invocation) {
        final Method method = invocation.getMethod();
        return new AccessPermission(
                ServiceProvider.fromClientClass(method.getDeclaringClass()),
                conformMethodName(method.getName())
        );
    }

    private static String conformMethodName(@NonNull final String methodName) {
        return methodName.substring(0, 1).toUpperCase() + methodName.substring(1);
    }
}
