package software.amazon.rds.globalcluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeGlobalClustersResponse;
import software.amazon.awssdk.services.rds.model.GlobalCluster;
import software.amazon.awssdk.services.rds.model.GlobalClusterMember;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.test.common.core.BaseProxyClient;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Mock
    RdsClient rds;

    private ReadHandler handler;

    @BeforeEach
    public void setup() {
        handler = new ReadHandler();
        rds = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyRdsClient = new BaseProxyClient<>(proxy, rds);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final DescribeGlobalClustersResponse describeGlobalClusterResponse = DescribeGlobalClustersResponse.builder().globalClusters(GLOBAL_CLUSTER_ACTIVE).build();
        when(proxyRdsClient.client().describeGlobalClusters(any(DescribeGlobalClustersRequest.class))).thenReturn(describeGlobalClusterResponse);
        final ResourceModel model = ResourceModel.builder().build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).describeGlobalClusters(any(DescribeGlobalClustersRequest.class));

        verify(rds, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rds);
    }

    @Test
    public void toResourceModel_PopulatesResourceModel_WithGlobalClusterIdentifier() {
        GlobalCluster cluster = GlobalCluster.builder()
                .globalClusterIdentifier("foo")
                .build();

        ResourceModel model = handler.toResourceModel(cluster);

        assertThat(model.getGlobalClusterIdentifier()).isEqualTo("foo");
    }

    @Test
    public void toResourceModel_PopulatesResourceModel_Engine() {
        GlobalCluster cluster = GlobalCluster.builder()
                .engine("aurora-mysql")
                .build();

        ResourceModel model = handler.toResourceModel(cluster);

        assertThat(model.getEngine()).isEqualTo("aurora-mysql");
    }

    @Test
    public void toResourceModel_PopulatesResourceModel_WithEngineVersion() {
        GlobalCluster cluster = GlobalCluster.builder()
                .engineVersion("5.7.mysql_aurora.2.07.2")
                .build();

        ResourceModel model = handler.toResourceModel(cluster);

        assertThat(model.getEngineVersion()).isEqualTo("5.7.mysql_aurora.2.07.2");
    }

    @Test
    public void toResourceModel_PopulatesResourceModel_WithStorageEncrypted() {
        GlobalCluster cluster = GlobalCluster.builder()
                .storageEncrypted(true)
                .build();

        ResourceModel model = handler.toResourceModel(cluster);

        assertThat(model.getStorageEncrypted()).isTrue();
    }

    @Test
    public void toResourceModel_PopulatesResourceModel_WithDeletingProtection() {
        GlobalCluster cluster = GlobalCluster.builder()
                .deletionProtection(true)
                .build();

        ResourceModel model = handler.toResourceModel(cluster);

        assertThat(model.getDeletionProtection()).isTrue();
    }

    @Test
    public void toResourceModel_PopulatesResourceModel_SourceDbClusterIdentifier() {
        GlobalClusterMember writer
                = GlobalClusterMember.builder().isWriter(true).dbClusterArn("arn:aws:rds::000000000000:global-cluster:cf-contract-test-global-cluster-0").build();

        GlobalClusterMember reader
                = GlobalClusterMember.builder().isWriter(false).dbClusterArn("arn:aws:rds::111111111111:global-cluster:cf-contract-test-global-cluster-0").build();

        GlobalCluster cluster = GlobalCluster.builder()
                .globalClusterMembers(new GlobalClusterMember[]{reader, writer, reader})
                .build();

        ResourceModel model = handler.toResourceModel(cluster);

        assertThat(model.getSourceDBClusterIdentifier()).isEqualTo("arn:aws:rds::000000000000:global-cluster:cf-contract-test-global-cluster-0");
    }
}
