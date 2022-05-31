package software.amazon.rds.dbcluster;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    RdsClient rdsClient;

    @Getter
    private ReadHandler handler;

    @BeforeEach
    public void setup() {
        handler = new ReadHandler();
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_ReadSuccess() {
        test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_RestoreOriginalIdentifier() {
        final String dbClusterIdentifier = "TestDBClusterIdentifier";
        final ProgressEvent<ResourceModel, CallbackContext> result = test_handleRequest_base(
                new CallbackContext(),
                () -> DBCLUSTER_ACTIVE.toBuilder().dbClusterIdentifier(dbClusterIdentifier.toLowerCase()).build(),
                () -> RESOURCE_MODEL.toBuilder().dBClusterIdentifier(dbClusterIdentifier).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));

        Assertions.assertThat(result.getResourceModel().getDBClusterIdentifier()).isEqualTo(dbClusterIdentifier);
    }
}
