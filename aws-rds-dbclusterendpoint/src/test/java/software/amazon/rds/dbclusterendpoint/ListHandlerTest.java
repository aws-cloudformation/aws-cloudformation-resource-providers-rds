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
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.HandlerConfig;

import java.time.Duration;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractHandlerTest {
    final String DB_CLUSTER_ENDPOINT_IDENTIFIER = "test-db-cluster-endpoint-identifier";
    final String DB_CLUSTER_IDENTIFIER = "test-db-cluster-identifier";
    final String ENDPOINT_TYPE = "ANY";

    final String DESCRIBE_DB_CLUSTER_ENDPOINTS_MARKER = "test-describe-db-cluster-endpoints-marker";
    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;
    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;
    @Mock
    private RdsClient rdsClient;
    @Getter
    private ListHandler handler;

    @BeforeEach
    public void setup() {
        handler = new ListHandler(HandlerConfig.builder().backoff(TEST_BACKOFF_DELAY).build());
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
        final DescribeDbClusterEndpointsResponse describeDbClusterEndpointsResponse = DescribeDbClusterEndpointsResponse.builder()
                .dbClusterEndpoints(Collections.singletonList(
                        DBClusterEndpoint.builder()
                                .dbClusterEndpointIdentifier(DB_CLUSTER_ENDPOINT_IDENTIFIER)
                                .dbClusterIdentifier(DB_CLUSTER_IDENTIFIER)
                                .customEndpointType(ENDPOINT_TYPE)
                                .build()
                ))
                .marker(DESCRIBE_DB_CLUSTER_ENDPOINTS_MARKER)
                .build();
        when(rdsProxy.client().describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class)))
                .thenReturn(describeDbClusterEndpointsResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_BUILDER().build(),
                expectSuccess()
        );

        final ResourceModel expectedModel = ResourceModel.builder()
                .dBClusterEndpointIdentifier(DB_CLUSTER_ENDPOINT_IDENTIFIER)
                .dBClusterIdentifier(DB_CLUSTER_IDENTIFIER)
                .endpointType(ENDPOINT_TYPE)
                .excludedMembers(Collections.emptySet())
                .staticMembers(Collections.emptySet())
                .build();

        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).containsExactly(expectedModel);
        assertThat(response.getNextToken()).isEqualTo(DESCRIBE_DB_CLUSTER_ENDPOINTS_MARKER);

        verify(rdsProxy.client()).describeDBClusterEndpoints(any(DescribeDbClusterEndpointsRequest.class));
    }
}
