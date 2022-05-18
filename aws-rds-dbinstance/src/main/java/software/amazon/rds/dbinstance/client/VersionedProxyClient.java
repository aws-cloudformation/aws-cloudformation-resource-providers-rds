package software.amazon.rds.dbinstance.client;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import lombok.NonNull;
import software.amazon.cloudformation.proxy.ProxyClient;

public class VersionedProxyClient<T> {

    private final Map<ApiVersion, ProxyClient<T>> clients = new HashMap<>();

    public VersionedProxyClient<T> register(final ApiVersion version, final ProxyClient<T> client) {
        clients.put(version, client);
        return this;
    }

    public ProxyClient<T> forVersion(@NonNull final ApiVersion apiVersion) {
        if (!clients.containsKey(apiVersion)) {
            throw new UnknownVersionException(apiVersion);
        }
        return clients.get(apiVersion);
    }

    public ProxyClient<T> defaultClient() {
        return forVersion(ApiVersion.DEFAULT);
    }
}
