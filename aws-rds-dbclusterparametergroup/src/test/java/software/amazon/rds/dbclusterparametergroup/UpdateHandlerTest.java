package software.amazon.rds.dbclusterparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.awssdk.services.rds.model.ResetDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.ResetDbClusterParameterGroupResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    private static final List<Tag> UPDATED_TAG_SET = Lists.newArrayList(Tag.builder().key("updated_key").value("updated_value").build());

    @Mock
    RdsClient rds;
    @Mock
    private AmazonWebServicesClientProxy proxy;
    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;
    private UpdateHandler handler;

    private ResourceModel RESOURCE_MODEL_PREV;
    private ResourceModel RESOURCE_MODEL;
    private ResourceModel UPDATED_RESOURCE_MODEL;

    private Map<String, Object> PARAMS;
    private Map<String, Object> UPDATED_PARAMS;

    private ResourceHandlerRequest<ResourceModel> requestSameParams;
    private ResourceHandlerRequest<ResourceModel> requestUpdParams;

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler(DefaultHandlerConfig.builder()
                .probingEnabled(false)
                .backoff(TEST_BACKOFF_DELAY)
                .stabilizationDelay(Duration.ZERO)
                .build());

        rds = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyRdsClient = MOCK_PROXY(proxy, rds);


        PARAMS = new HashMap<>();
        PARAMS.put("param", "value");
        PARAMS.put("param2", "value");

        UPDATED_PARAMS = new HashMap<>();
        UPDATED_PARAMS.put("param", "value");

        RESOURCE_MODEL_PREV = ResourceModel.builder()
                .parameters(null)
                .build();


        RESOURCE_MODEL = ResourceModel.builder()
                .description(DESCRIPTION)
                .dBClusterParameterGroupName("SampleName")
                .family(FAMILY)
                .parameters(PARAMS)
                .tags(TAG_SET)
                .build();
        UPDATED_RESOURCE_MODEL = ResourceModel.builder()
                .description(UPDATED_DESCRIPTION)
                .dBClusterParameterGroupName("SampleName")
                .family(FAMILY)
                .parameters(UPDATED_PARAMS)
                .tags(UPDATED_TAG_SET)
                .build();

        requestSameParams = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .desiredResourceState(RESOURCE_MODEL)
                .previousResourceState(RESOURCE_MODEL)
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .logicalResourceIdentifier("logicalId").build();
        requestUpdParams = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .desiredResourceState(UPDATED_RESOURCE_MODEL)
                .previousResourceState(RESOURCE_MODEL_PREV)
                .desiredResourceTags(translateTagsToMap(UPDATED_TAG_SET))
                .logicalResourceIdentifier("logicalId").build();
    }

    @AfterEach
    public void post_execute() {
        verify(rds, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(proxyRdsClient.client());
    }

    @Test
    public void handleRequest_Success() {

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        final DescribeDbClusterParameterGroupsResponse describeDbClusterParameterGroupsResponse = DescribeDbClusterParameterGroupsResponse.builder()
                .dbClusterParameterGroups(DBClusterParameterGroup.builder()
                        .dbClusterParameterGroupArn("arn").build()).build();
        when(rds.describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(rds.listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rds.addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, requestUpdParams, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(proxyRdsClient.client()).resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class));
        verify(proxyRdsClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_stabilized() {
        when(rds.resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class)))
                .thenReturn(ResetDbClusterParameterGroupResponse.builder().build());

        when(rds.describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class)))
                .thenReturn(DescribeDbClusterParametersResponse.builder()
                        .parameters(translateParamMapToCollection(PARAMS))
                        .build());

        when(rds.describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class)))
                .thenReturn(DescribeDbClusterParameterGroupsResponse.builder()
                        .dbClusterParameterGroups(DBClusterParameterGroup.builder()
                                .dbClusterParameterGroupName("group")
                                .dbClusterParameterGroupArn("arn")
                                .build())
                        .build());

        when(rds.addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rds.removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(rds.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final ResourceHandlerRequest<ResourceModel> handlerRequest = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .previousResourceState(RESOURCE_MODEL.toBuilder().dBClusterParameterGroupName("group").parameters(PARAMS).tags(TAG_SET).build())
                .desiredResourceState(UPDATED_RESOURCE_MODEL.toBuilder().dBClusterParameterGroupName("group").parameters(UPDATED_PARAMS).tags(UPDATED_TAG_SET).build())
                .logicalResourceIdentifier("logicalId")
                .build();

        final CallbackContext context = new CallbackContext();

        final ProgressEvent<ResourceModel, CallbackContext> handlerResponse = handler.handleRequest(proxy, handlerRequest, context, proxyRdsClient, logger);

        assertThat(handlerResponse).isNotNull();
        assertThat(handlerResponse.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(handlerResponse.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(handlerResponse.getResourceModels()).isNull();
        assertThat(handlerResponse.getMessage()).isNull();
        assertThat(handlerResponse.getErrorCode()).isNull();
        assertThat(handlerResponse.getCallbackContext().isParameterGroupStabilized()).isTrue();

        verify(proxyRdsClient.client()).resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class));
        verify(proxyRdsClient.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(proxyRdsClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyRdsClient.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(proxyRdsClient.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
    }
}
