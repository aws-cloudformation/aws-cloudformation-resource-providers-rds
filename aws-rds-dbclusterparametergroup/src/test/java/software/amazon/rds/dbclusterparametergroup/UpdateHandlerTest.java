package software.amazon.rds.dbclusterparametergroup;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DbClusterNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.awssdk.services.rds.model.ResetDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.ResetDbClusterParameterGroupResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.exceptions.CfnGeneralServiceException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Mock
    RdsClient rds;

    private UpdateHandler handler;

    private ResourceModel RESOURCE_MODEL_PREV;
    private ResourceModel RESOURCE_MODEL;

    private Map<String, Object> PARAMS;

    private ResourceHandlerRequest<ResourceModel> requestSameParams;
    private ResourceHandlerRequest<ResourceModel> requestUpdParams;
    private static final String ACCESS_DENIED_ERROR_CODE = "AccessDenied";
    private static final String NOT_ACCESS_DENIED_ERROR_CODE = "DBClusterNotFoundFault";

    @AfterEach
    public void post_execute() {
        verify(rds, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(proxyRdsClient.client());
    }

    @BeforeEach
    public void setup() {

        handler = new UpdateHandler();
        rds = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyRdsClient = proxyRdsClient = MOCK_PROXY(proxy, rds);


        PARAMS = new HashMap<>();
        PARAMS.put("param", "value");
        PARAMS.put("param2", "value");


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

        requestSameParams = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .desiredResourceState(RESOURCE_MODEL)
                .previousResourceState(RESOURCE_MODEL)
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .logicalResourceIdentifier("logicalId").build();
        requestUpdParams = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .desiredResourceState(RESOURCE_MODEL)
                .previousResourceState(RESOURCE_MODEL_PREV)
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .logicalResourceIdentifier("logicalId").build();
    }

    @Test
    public void handleRequest_SimpleSuccess(){

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);
        callbackContext.setClusterStabilized(true);

        final ResetDbClusterParameterGroupResponse resetDbClusterParameterGroupResponse = ResetDbClusterParameterGroupResponse.builder().build();
        when(rds.resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class))).thenReturn(resetDbClusterParameterGroupResponse);
        final DescribeDbClusterParameterGroupsResponse describeDbClusterParameterGroupsResponse = DescribeDbClusterParameterGroupsResponse.builder()
                .dbClusterParameterGroups(DBClusterParameterGroup.builder()
                        .dbClusterParameterGroupArn("arn").build()).build();
        when(rds.describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(rds.listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);
        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(rds.removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rds.addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, requestUpdParams, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class));
        verify(proxyRdsClient.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(proxyRdsClient.client(), times(2)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyRdsClient.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyRdsClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_StabilizationWithNextPage(){

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);
        callbackContext.setClusterStabilized(false);

        final DBCluster dbCluster = DBCluster.builder()
                .dbClusterParameterGroup("SampleName")
                .status("available").build();

        final ResetDbClusterParameterGroupResponse resetDbClusterParameterGroupResponse = ResetDbClusterParameterGroupResponse.builder().build();
        when(rds.resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class))).thenReturn(resetDbClusterParameterGroupResponse);
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder()
                .dbClusters(Lists.newArrayList(dbCluster))
                .marker("token")
                .build();
        when(rds.describeDBClusters(any(DescribeDbClustersRequest.class))).thenReturn(describeDbClustersResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, requestUpdParams, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(30);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getNextToken()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class));
        verify(proxyRdsClient.client()).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_Stabilization_ThrowsException(){

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);
        callbackContext.setClusterStabilized(false);

        final ResetDbClusterParameterGroupResponse resetDbClusterParameterGroupResponse = ResetDbClusterParameterGroupResponse.builder().build();
        when(rds.resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class))).thenReturn(resetDbClusterParameterGroupResponse);
        when(rds.describeDBClusters(any(DescribeDbClustersRequest.class))).thenThrow(RdsException.builder()
                .statusCode(404)
                .awsErrorDetails(AwsErrorDetails.builder()
                        .errorCode(NOT_ACCESS_DENIED_ERROR_CODE)
                        .errorMessage("DBClusterIdentifier doesn't refer to an existing DB cluster")
                        .serviceName("RDS")
                        .build())
                .build());
        assertThatThrownBy(() -> handler.handleRequest(proxy, requestUpdParams, callbackContext, proxyRdsClient, logger))
                .hasMessage("DBClusterIdentifier doesn't refer to an existing DB cluster (Service: RDS, Status Code: 404, Request ID: null, Extended Request ID: null)")
                .isExactlyInstanceOf(CfnGeneralServiceException.class);
    }

    @Test
    public void handleRequest_Stabilization_ThrowsException_withoutAWSErrorDetails(){

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);
        callbackContext.setClusterStabilized(false);

        final ResetDbClusterParameterGroupResponse resetDbClusterParameterGroupResponse = ResetDbClusterParameterGroupResponse.builder().build();
        when(rds.resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class))).thenReturn(resetDbClusterParameterGroupResponse);
        when(rds.describeDBClusters(any(DescribeDbClustersRequest.class))).thenThrow(RdsException.builder()
                .message("DBClusterIdentifier doesn't refer to an existing DB cluster")
                .statusCode(404)
                .build());
        assertThatThrownBy(() -> handler.handleRequest(proxy, requestUpdParams, callbackContext, proxyRdsClient, logger))
                .hasMessage("DBClusterIdentifier doesn't refer to an existing DB cluster")
                .isExactlyInstanceOf(CfnGeneralServiceException.class);
    }

    @Test
    public void handleRequest_StabilizationSoftFail_success(){

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);
        callbackContext.setClusterStabilized(false);

        final ResetDbClusterParameterGroupResponse resetDbClusterParameterGroupResponse = ResetDbClusterParameterGroupResponse.builder().build();
        when(rds.resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class))).thenReturn(resetDbClusterParameterGroupResponse);
        when(rds.describeDBClusters(any(DescribeDbClustersRequest.class))).thenThrow(RdsException.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorCode(ACCESS_DENIED_ERROR_CODE).build())
                .build()
        );
        final DescribeDbClusterParameterGroupsResponse describeDbClusterParameterGroupsResponse = DescribeDbClusterParameterGroupsResponse.builder()
                .dbClusterParameterGroups(DBClusterParameterGroup.builder()
                        .dbClusterParameterGroupArn("arn")
                        .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                        .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                        .description(RESOURCE_MODEL.getDescription()).build()).build();
        when(proxyRdsClient.client().describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);
        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tagList(Tag.builder().key("key").value("value").build()).build();
        when(proxyRdsClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);
        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(rds.removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rds.addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, requestUpdParams, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getNextToken()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class));
        verify(proxyRdsClient.client()).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(proxyRdsClient.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(proxyRdsClient.client(), times(2)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyRdsClient.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyRdsClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_StabilizationWithoutNextPage(){

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);
        callbackContext.setClusterStabilized(false);

        final DBCluster dbCluster = DBCluster.builder()
                .dbClusterParameterGroup("SampleName")
                .status("available").build();

        final ResetDbClusterParameterGroupResponse resetDbClusterParameterGroupResponse = ResetDbClusterParameterGroupResponse.builder().build();
        when(rds.resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class))).thenReturn(resetDbClusterParameterGroupResponse);
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder()
                .dbClusters(Lists.newArrayList(dbCluster))
                .build();
        when(rds.describeDBClusters(any(DescribeDbClustersRequest.class))).thenReturn(describeDbClustersResponse);
        final DescribeDbClusterParameterGroupsResponse describeDbClusterParameterGroupsResponse = DescribeDbClusterParameterGroupsResponse.builder()
                .dbClusterParameterGroups(DBClusterParameterGroup.builder()
                        .dbClusterParameterGroupArn("arn")
                        .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                        .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                        .description(RESOURCE_MODEL.getDescription()).build()).build();
        when(proxyRdsClient.client().describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);
        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tagList(Tag.builder().key("key").value("value").build()).build();
        when(proxyRdsClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);
        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(rds.removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rds.addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, requestUpdParams, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getNextToken()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class));
        verify(proxyRdsClient.client()).describeDBClusters(any(DescribeDbClustersRequest.class));
        verify(proxyRdsClient.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(proxyRdsClient.client(), times(2)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyRdsClient.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyRdsClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_StabilizationModifying(){

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);
        callbackContext.setClusterStabilized(false);

        final DBCluster dbCluster = DBCluster.builder()
                .dbClusterParameterGroup("SampleName")
                .status("modifying").build();

        final ResetDbClusterParameterGroupResponse resetDbClusterParameterGroupResponse = ResetDbClusterParameterGroupResponse.builder().build();
        when(rds.resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class))).thenReturn(resetDbClusterParameterGroupResponse);
        final DescribeDbClustersResponse describeDbClustersResponse = DescribeDbClustersResponse.builder()
                .dbClusters(Lists.newArrayList(dbCluster))
                .build();
        when(rds.describeDBClusters(any(DescribeDbClustersRequest.class))).thenReturn(describeDbClustersResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, requestUpdParams, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(30);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getNextToken()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).resetDBClusterParameterGroup(any(ResetDbClusterParameterGroupRequest.class));
        verify(proxyRdsClient.client()).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessSameParams(){
        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);
        callbackContext.setClusterStabilized(true);

        final DescribeDbClusterParameterGroupsResponse describeDbClusterParameterGroupsResponse = DescribeDbClusterParameterGroupsResponse.builder()
                .dbClusterParameterGroups(DBClusterParameterGroup.builder()
                        .dbClusterParameterGroupArn("arn").build()).build();
        when(rds.describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(rds.listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);
        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(rds.removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rds.addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, requestSameParams, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(proxyRdsClient.client(), times(2)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyRdsClient.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyRdsClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
    }
}
