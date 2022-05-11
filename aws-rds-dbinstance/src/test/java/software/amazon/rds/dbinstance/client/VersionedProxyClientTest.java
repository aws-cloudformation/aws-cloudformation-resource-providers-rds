package software.amazon.rds.dbinstance.client;

import java.util.function.Function;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.cloudformation.proxy.ProxyClient;

class VersionedProxyClientTest {

    @Test
    public void test_forVersion_noClientsRegistered_ThrowsException() {
        Assertions.assertThatExceptionOfType(UnknownVersionException.class).isThrownBy(() -> {
            final VersionedProxyClient<Void> client = new VersionedProxyClient<Void>();
            client.forVersion(ApiVersion.DEFAULT);
        });
    }

    @Test
    public void test_forVersion_null() {
        Assertions.assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
            final VersionedProxyClient<Void> client = new VersionedProxyClient<Void>();
            client.forVersion(null);
        });
    }

    @Test
    public void test_defaultClient_noClientsRegistered_ThrowsException() {
        Assertions.assertThatExceptionOfType(UnknownVersionException.class).isThrownBy(() -> {
            final VersionedProxyClient<Void> client = new VersionedProxyClient<Void>();
            client.defaultClient();
        });
    }

    @Test
    public void test_forVersion_registeredClient() {
        final VersionedProxyClient<Void> client = new VersionedProxyClient<Void>();
        final TestClient proxyClient = new TestClient();
        client.register(ApiVersion.V12, proxyClient);
        Assertions.assertThat(client.forVersion(ApiVersion.V12)).isEqualTo(proxyClient);
    }

    @Test
    public void test_defaultClient_registeredClient() {
        final VersionedProxyClient<Void> client = new VersionedProxyClient<Void>();
        final TestClient proxyClient = new TestClient();
        client.register(ApiVersion.DEFAULT, proxyClient);
        Assertions.assertThat(client.defaultClient()).isEqualTo(proxyClient);
    }

    private static class TestClient implements ProxyClient<Void> {

        @Override
        public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseT injectCredentialsAndInvokeV2(RequestT request, Function<RequestT, ResponseT> requestFunction) {
            return null;
        }

        @Override
        public Void client() {
            return null;
        }
    }
}
