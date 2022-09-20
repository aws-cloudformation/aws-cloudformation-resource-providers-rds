package software.amazon.rds.test.common.verification;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.mockito.exceptions.base.MockitoAssertionError;
import org.mockito.internal.verification.api.VerificationData;
import org.mockito.invocation.Invocation;
import org.mockito.verification.VerificationMode;

import software.amazon.rds.test.common.annotations.ExcludeFromJacocoGeneratedReport;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.ServiceProvider;

public class AccessPermissionVerificationMode implements VerificationMode {

    private final Set<AccessPermission> permissions;

    public AccessPermissionVerificationMode() {
        this.permissions = new HashSet<>();
    }

    public AccessPermissionVerificationMode enablePermission(final AccessPermission permission) {
        this.permissions.add(permission);
        return this;
    }

    @ExcludeFromJacocoGeneratedReport
    public AccessPermissionVerificationMode withDefaultPermissions() {
        this.permissions.add(new AccessPermission(ServiceProvider.SDK, "ServiceName"));
        return this;
    }

    @ExcludeFromJacocoGeneratedReport
    public AccessPermissionVerificationMode withSchemaPermissions(final JSONObject schema, final HandlerName handlerName) {
        final JSONArray schemaPermissions = schema.getJSONObject("handlers")
                .getJSONObject(handlerName.toString())
                .getJSONArray("permissions");

        for (final Object permissionHandle : schemaPermissions) {
            final AccessPermission permission = AccessPermissionFactory.fromString((String) permissionHandle);
            this.enablePermission(permission);
        }

        return this;
    }

    private MockitoAssertionError missingRequiredPermission(final AccessPermission permission) {
        return new MockitoAssertionError(String.format("Missing a required access permission: %s", permission));
    }

    private void verifyInvocationPermissions(final Invocation invocation) {
        final AccessPermission requiredPermission = AccessPermissionFactory.fromInvocation(invocation);
        if (!permissions.contains(requiredPermission)) {
            throw missingRequiredPermission(requiredPermission);
        }
    }

    @Override
    public void verify(final VerificationData data) {
        final List<Invocation> invocations = data.getAllInvocations();
        for (final Invocation invocation : invocations) {
            verifyInvocationPermissions(invocation);
        }
    }
}
