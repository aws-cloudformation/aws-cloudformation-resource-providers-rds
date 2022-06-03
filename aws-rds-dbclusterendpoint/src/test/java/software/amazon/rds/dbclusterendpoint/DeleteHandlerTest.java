package software.amazon.rds.dbclusterendpoint;

import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterEndpoint;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterEndpointRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterEndpointResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.HandlerConfig;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    RdsClient rdsClient;

    @Getter
    private DeleteHandler handler;

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler(HandlerConfig.builder().backoff(TEST_BACKOFF_DELAY).build());
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        rdsProxy = mockProxy(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        when(rdsProxy.client().deleteDBClusterEndpoint(any(DeleteDbClusterEndpointRequest.class)))
                .thenReturn(DeleteDbClusterEndpointResponse.builder().build());
        when(rdsProxy.client().describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class)))
                .thenReturn(DescribeDbClusterEndpointsResponse.builder()
                        .dbClusterEndpoints(new DBClusterEndpoint[]{}).build());

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        assertThat(response.getMessage()).isNull();

        verify(rdsProxy.client(), times(1)).deleteDBClusterEndpoint(any(DeleteDbClusterEndpointRequest.class));
    }

    @Test
    public void handleRequest_IsDeleting_stabilize() {

        final DeleteDbClusterEndpointResponse deleteDbClusterEndpointResponse = DeleteDbClusterEndpointResponse.builder().build();
        when(rdsProxy.client().deleteDBClusterEndpoint(any(DeleteDbClusterEndpointRequest.class))).thenReturn(deleteDbClusterEndpointResponse);

        AtomicBoolean fetchedOnce = new AtomicBoolean(false);
        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (fetchedOnce.compareAndSet(false, true)) {
                        return DB_CLUSTER_ENDPOINT_DELETING;
                    }
                    return null;
                },
                () -> RESOURCE_MODEL_BUILDER().build(),
                expectSuccess()
        );

        assertThat(response.getMessage()).isNull();

        verify(rdsProxy.client(), times(1)).deleteDBClusterEndpoint(any(DeleteDbClusterEndpointRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class));
    }
}
