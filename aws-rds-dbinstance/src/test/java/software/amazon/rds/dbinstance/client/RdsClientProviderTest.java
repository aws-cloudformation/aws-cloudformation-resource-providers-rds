package software.amazon.rds.dbinstance.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static software.amazon.rds.common.client.RdsUserAgentProvider.SDK_CLIENT_USER_AGENT_PREFIX;

import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;

class RdsClientProviderTest {

    @BeforeEach
    public void setup() {
        System.setProperty("aws.region", "us-east-1");
        System.setProperty("aws.accessKeyId", "test-access-key-id");
        System.setProperty("aws.secretAccessKey", "test-access-key");
    }

    @AfterEach
    public void tearDown() {
        System.clearProperty("aws.region");
        System.clearProperty("aws.accessKeyId");
        System.clearProperty("aws.secretAccessKey");
    }

    @Test
    public void test_getClient() {
        Assertions.assertThat(new RdsClientProvider().getClient()).isNotNull();
    }

    @Test
    public void test_getClientWithApiVersion_null() {
        Assertions.assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> {
            new RdsClientProvider().getClientForApiVersion(null);
        });
    }

    // This method is going way too far into the SDK implementation details.
    // As far as we need to verify a specific query parameter is set by the interceptor, there seems to be
    // no easier way to verify it rather than invoke the entire call chain.
    // This test could be sensitive to the exact SDK implementation.
    // According to the SDK Core team, overriding the client parameters using an interceptor is the only available way.
    // Unfortunately, workability of this approach depends on the interceptor call order, which could be changed in the future.
    // Upon this test failure, one needs to ensure the new SDK version is compatible with this interceptor-based approach.
    @Test
    public void test_getClientWithApiVersion_fullExec() throws IOException {
        // This is the minimal valid XML that is accepted by the SDK parser.
        final AbortableInputStream responseBodyStream = AbortableInputStream.create(
                IOUtils.toInputStream("<DescribeDBInstancesResponse/>", Charset.defaultCharset())
        );
        final SdkHttpResponse sdkHttpResponse = SdkHttpResponse.builder()
                .statusCode(200)
                .content(responseBodyStream).build();
        final HttpExecuteResponse httpExecuteResponse = HttpExecuteResponse.builder()
                .response(sdkHttpResponse)
                .responseBody(responseBodyStream)
                .build();
        final ExecutableHttpRequest executableHttpRequest = mock(ExecutableHttpRequest.class);
        when(executableHttpRequest.call()).thenReturn(httpExecuteResponse);

        final SdkHttpClient sdkHttpClient = mock(SdkHttpClient.class);
        ArgumentCaptor<HttpExecuteRequest> requestArgumentCaptor = ArgumentCaptor.forClass(HttpExecuteRequest.class);
        when(sdkHttpClient.prepareRequest(requestArgumentCaptor.capture())).thenReturn(executableHttpRequest);

        final String apiVersion = "2012-10-31";
        final RdsClient client = new RdsClientProvider(() -> sdkHttpClient).getClientForApiVersion(apiVersion);
        Assertions.assertThat(client).isNotNull();

        // The exact method doesn't matter.
        client.describeDBInstances(DescribeDbInstancesRequest.builder().build());

        // Read the request body input stream entirely.
        final HttpExecuteRequest httpExecuteRequest = requestArgumentCaptor.getValue();
        byte[] requestBytes = IOUtils.toByteArray(httpExecuteRequest.contentStreamProvider().get().newStream());
        final String requestBody = new String(requestBytes);

        //TODO: Ensure the client encoded {@code apiVersion} into the request POST body.
        //Assertions.assertThat(requestBody).contains("Version=" + apiVersion);

        // Ensure UserAgent is modified to reflect the resource handler client signature.
        final String userAgent = httpExecuteRequest.httpRequest().headers().get("User-Agent").get(0);
        Assertions.assertThat(userAgent).startsWith(SDK_CLIENT_USER_AGENT_PREFIX);
    }
}
