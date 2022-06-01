package software.amazon.rds.dbclusterendpoint;

import java.time.Duration;

import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbClusterEndpointRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterEndpointResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    @Getter
    private CreateHandler handler;

    @BeforeEach
    public void setup() {
        handler = new CreateHandler();
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_CreateSuccess() {
        when(proxyClient.client().createDBClusterEndpoint(any(CreateDbClusterEndpointRequest.class)))
                .thenReturn(CreateDbClusterEndpointResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DB_CLUSTER_ENDPOINT_AVAILABLE,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).createDBClusterEndpoint(any(CreateDbClusterEndpointRequest.class));
    }
}
