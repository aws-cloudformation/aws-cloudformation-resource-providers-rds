package software.amazon.rds.dbclusterparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;

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

        handler = new ReadHandler(DefaultHandlerConfig.builder()
                .probingEnabled(false)
                .backoff(TEST_BACKOFF_DELAY)
                .stabilizationDelay(Duration.ZERO)
                .build());
        rds = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyRdsClient = MOCK_PROXY(proxy, rds);
    }

    @AfterEach
    public void post_execute() {
        verify(rds, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(proxyRdsClient.client());
    }

    @Test
    public void handleRequest_Success() {
        final DescribeDbClusterParameterGroupsResponse describeDbClusterParameterGroupsResponse = DescribeDbClusterParameterGroupsResponse.builder()
                .dbClusterParameterGroups(DBClusterParameterGroup.builder()
                        .dbClusterParameterGroupArn("arn")
                        .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                        .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                        .description(RESOURCE_MODEL.getDescription()).build()).build();
        when(rds.describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);
        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tagList(Tag.builder().key("key").value("value").build()).build();
        when(rds.listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModel()).isEqualTo(request.getDesiredResourceState());
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(proxyRdsClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }
}
