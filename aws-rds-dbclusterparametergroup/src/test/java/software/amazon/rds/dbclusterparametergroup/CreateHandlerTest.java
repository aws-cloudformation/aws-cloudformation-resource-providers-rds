package software.amazon.rds.dbclusterparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.CreateDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterParameterGroupResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterParameterGroupResponse;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    @Getter
    RdsClient rdsClient;

    @Getter
    private CreateHandler handler;

    private ResourceModel RESOURCE_MODEL;

    private Map<String, Object> PARAMS;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.CREATE;
    }

    @BeforeEach
    public void setup() {
        handler = new CreateHandler(HandlerConfig.builder()
                .backoff(Constant.of()
                        .delay(Duration.ofSeconds(1))
                        .timeout(Duration.ofSeconds(120))
                        .build())
                .build());

        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = MOCK_PROXY(proxy, rdsClient);


        PARAMS = new HashMap<>();
        PARAMS.put("param", "value");
        PARAMS.put("param2", "value");


        RESOURCE_MODEL = ResourceModel.builder()
                .description(DESCRIPTION)
                .dBClusterParameterGroupName(null)
                .family(FAMILY)
                .parameters(PARAMS)
                .tags(TAG_SET)
                .build();
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsProxy.client());
        verifyAccessPermissions(rdsProxy.client());
    }

    @Test
    public void handleRequest_Success() {
        when(rdsProxy.client().createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tagList(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())
                        .build());

        final DBClusterParameterGroup dbClusterParameterGroup = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn(ARN)
                .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                .description(RESOURCE_MODEL.getDescription()).build();

        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        test_handleRequest_base(
                callbackContext,
                () -> dbClusterParameterGroup,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(rdsProxy.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_UnauthorizedTaggingOperationOnCreate() {
        when(rdsProxy.client().createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenThrow(RdsException.builder()
                        .message("Role not authorized to execute rds:AddTagsToResource")
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.AccessDeniedException.toString())
                                .build()
                        ).build());


        Map<String, String> stackTags = ImmutableMap.of(TAG_KEY, TAG_VALUE);
        Map<String, String> systemTags = ImmutableMap.of("systemTag1", "value2", "systemTag2", "value3");
        Map<String, String> allTags = ImmutableMap.<String, String>builder()
                .putAll(stackTags)
                .putAll(systemTags)
                .build();

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                callbackContext,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(systemTags)
                        .desiredResourceTags(stackTags),
                null,
                null,
                () -> RESOURCE_MODEL.toBuilder()
                        .tags(null)
                        .build(),
                expectFailed(HandlerErrorCode.UnauthorizedTaggingOperation)
        );

        ArgumentCaptor<CreateDbClusterParameterGroupRequest> createCaptor = ArgumentCaptor.forClass(CreateDbClusterParameterGroupRequest.class);
        verify(rdsProxy.client(), times(1)).createDBClusterParameterGroup(createCaptor.capture());

        final CreateDbClusterParameterGroupRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(asSdkTagArray(allTags));
    }

    @Test
    public void handleRequest_FailWithAccessDenied() {
        when(rdsProxy.client().createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenThrow(AwsServiceException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorMessage("Access denied")
                                .errorCode(HandlerErrorCode.AccessDenied.toString())
                                .build())
                        .build());

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        test_handleRequest_base(
                callbackContext,
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.AccessDenied)
        );

        verify(rdsProxy.client(), times(1)).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
    }

    @Test
    public void handleRequest_MissingDescribeDBClusterParameterGroupsPermission() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        when(rdsClient.modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class)))
                .thenReturn(ModifyDbClusterParameterGroupResponse.builder().build());

        when(rdsClient.describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class)))
                .thenThrow(RdsException.builder().awsErrorDetails(
                        AwsErrorDetails.builder().errorCode(HandlerErrorCode.AccessDenied.toString()).build()
                ).build());

        when(rdsClient.describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(DBCluster.builder().dbClusterParameterGroup("group").status("available").build())
                        .build());

        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        mockDescribeDbClusterParametersResponse("static", "dynamic", true);

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_ThrottleOnDescribeDBClusters() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        when(rdsClient.modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class)))
                .thenReturn(ModifyDbClusterParameterGroupResponse.builder().build());

        when(rdsClient.describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenThrow(AwsServiceException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(HandlerErrorCode.Throttling.toString())
                                .build())
                        .build());

        mockDescribeDbClusterParametersResponse("static", "dynamic", true);

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.Throttling)
        );

        verify(rdsProxy.client(), times(1)).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_SuccessWithParameters() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build()
                );

        when(rdsClient.modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class)))
                .thenReturn(ModifyDbClusterParameterGroupResponse.builder().build());

        when(rdsClient.describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(DBCluster.builder().dbClusterParameterGroup("group").status("available").build())
                        .build());

        final DBClusterParameterGroup dbClusterParameterGroup = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn(ARN)
                .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                .description(RESOURCE_MODEL.getDescription()).build();

        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tagList(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())
                        .build());

        mockDescribeDbClusterParametersResponse("static", "dynamic", true);

        test_handleRequest_base(
                new CallbackContext(),
                () -> dbClusterParameterGroup,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_SuccessWithEmptyParameters() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        when(rdsClient.describeDBClusters(any(DescribeDbClustersRequest.class)))
                .thenReturn(DescribeDbClustersResponse.builder()
                        .dbClusters(DBCluster.builder().dbClusterParameterGroup("group").status("available").build())
                        .build());

        final DBClusterParameterGroup dbClusterParameterGroup = DBClusterParameterGroup.builder()
                .dbClusterParameterGroupArn(ARN)
                .dbClusterParameterGroupName(RESOURCE_MODEL.getDBClusterParameterGroupName())
                .dbParameterGroupFamily(RESOURCE_MODEL.getFamily())
                .description(RESOURCE_MODEL.getDescription()).build();

        when(rdsProxy.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder()
                        .tagList(Tag.builder().key(TAG_KEY).value(TAG_VALUE).build())
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> dbClusterParameterGroup,
                () -> RESOURCE_MODEL.toBuilder()
                        .parameters(Collections.emptyMap())
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client()).describeDBClusters(any(DescribeDbClustersRequest.class));
    }

    @Test
    public void handleRequest_InProgressFailedUnmodifiableParams() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        mockDescribeDbClusterParametersResponse("static", "dynamic", false);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.InvalidRequest)
        );

        assertThat(response.getMessage()).isEqualTo("Invalid / Unmodifiable / Unsupported DB Parameter: param");

        verify(rdsProxy.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class));
    }

    @Test
    public void handleRequest_InProgressFailedUnsupportedParams() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        mockDescribeDbClusterParametersResponse("static", "dynamic", true);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL.toBuilder().parameters(Collections.singletonMap("Wrong Key", "Wrong value")).build(),
                expectFailed(HandlerErrorCode.InvalidRequest)
        );

        assertThat(response.getMessage()).isEqualTo("Invalid / Unmodifiable / Unsupported DB Parameter: Wrong Key");

        verify(rdsProxy.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class));
    }

    @Test
    public void handleRequest_ThrottlingFailure() {
        when(rdsClient.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class)))
                .thenReturn(CreateDbClusterParameterGroupResponse.builder()
                        .dbClusterParameterGroup(DB_CLUSTER_PARAMETER_GROUP)
                        .build());

        when(rdsProxy.client().describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class)))
                .thenThrow(RdsException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(HandlerErrorCode.Throttling.toString())
                                .build())
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.Throttling)
        );

        verify(rdsProxy.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(rdsProxy.client()).describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class));
    }

    private void mockDescribeDbClusterParametersResponse(
            final String firstParamApplyType,
            final String secondParamApplyType,
            final boolean isModifiable
    ) {
        Parameter param1 = Parameter.builder()
                .parameterName("param")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(firstParamApplyType)
                .build();
        Parameter param2 = Parameter.builder()
                .parameterName("param2")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(secondParamApplyType)
                .build();

        final DescribeDbClusterParametersResponse firstPage = DescribeDbClusterParametersResponse.builder()
                .marker("marker")
                .parameters(param1, param2).build();


        final DescribeDbClusterParametersResponse lastPage = DescribeDbClusterParametersResponse.builder().build();

        when(rdsProxy.client().describeDBClusterParameters(any(DescribeDbClusterParametersRequest.class)))
                .thenReturn(firstPage)
                .thenReturn(lastPage);
    }

    private Tag[] asSdkTagArray(final Map<String, String> tags) {
        return Iterables.toArray(Tagging.translateTagsToSdk(tags), software.amazon.awssdk.services.rds.model.Tag.class);
    }
}
