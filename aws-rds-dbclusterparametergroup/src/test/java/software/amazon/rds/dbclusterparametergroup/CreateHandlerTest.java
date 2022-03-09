package software.amazon.rds.dbclusterparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateDbClusterParameterGroupResponse;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClustersResponse;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultClusterParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultClusterParametersResponse;
import software.amazon.awssdk.services.rds.model.EngineDefaults;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbClusterParameterGroupResponse;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.ResetDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.awssdk.services.rds.paginators.DescribeDBClusterParametersIterable;
import software.amazon.awssdk.services.rds.paginators.DescribeDBClustersIterable;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Mock
    RdsClient rds;

    private CreateHandler handler;

    private ResourceModel RESOURCE_MODEL;

    private Map<String, Object> PARAMS;
    private final String StackId = "arn:aws:cloudformation:us-east-1:123456789:stack/MyStack/aaf549a0-a413-11df-adb3-5081b3858e83";

    @AfterEach
    public void post_execute() {
        verify(rds, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(proxyRdsClient.client());
    }

    @BeforeEach
    public void setup() {

        handler = new CreateHandler();
        rds = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyRdsClient = MOCK_PROXY(proxy, rds);


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

    @Test
    public void handleRequest_SimpleSuccess() {

        final CreateDbClusterParameterGroupResponse createDbClusterParameterGroupResponse = CreateDbClusterParameterGroupResponse.builder().build();
        when(proxyRdsClient.client().createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class))).thenReturn(createDbClusterParameterGroupResponse);

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

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId(StackId)
                .logicalResourceIdentifier("logicalId").build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(proxyRdsClient.client()).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
        verify(proxyRdsClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessWithParameters() {
        final CreateHandler handler = new CreateHandler();
        final CreateDbClusterParameterGroupResponse createDbClusterParameterGroupResponse = CreateDbClusterParameterGroupResponse.builder().build();
        when(rds.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class))).thenReturn(createDbClusterParameterGroupResponse);
        final ModifyDbClusterParameterGroupResponse modifyDbClusterParameterGroupResponse = ModifyDbClusterParameterGroupResponse.builder().build();
        when(rds.modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class))).thenReturn(modifyDbClusterParameterGroupResponse);
        final DescribeDBClustersIterable describeDbClustersResponse = mock(DescribeDBClustersIterable.class);
        when(describeDbClustersResponse.stream())
                .thenReturn(Stream.<DescribeDbClustersResponse>builder()
                        .add(DescribeDbClustersResponse.builder().dbClusters(DBCluster.builder().dbClusterParameterGroup("group").build()).build())
                        .build()
                );
        when(rds.describeDBClustersPaginator(any(DescribeDbClustersRequest.class))).thenReturn(describeDbClustersResponse);
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
        mockDescribeDbClusterParametersResponse("static", "dynamic", true);

        RESOURCE_MODEL.setDBClusterParameterGroupName("sampleName");
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .desiredResourceState(RESOURCE_MODEL)
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .stackId(StackId)
                .logicalResourceIdentifier("logicalId").build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(proxyRdsClient.client()).modifyDBClusterParameterGroup(any(ModifyDbClusterParameterGroupRequest.class));
        verify(proxyRdsClient.client()).describeDBClustersPaginator(any(DescribeDbClustersRequest.class));
    }


    @Test
    public void handleRequest_SimpleInProgressFailedUnmodifiableParams() {
        final CreateHandler handler = new CreateHandler();
        final CreateDbClusterParameterGroupResponse createDbClusterParameterGroupResponse = CreateDbClusterParameterGroupResponse.builder().build();
        when(rds.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class))).thenReturn(createDbClusterParameterGroupResponse);
        mockDescribeDbClusterParametersResponse("static", "dynamic", false);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .desiredResourceState(RESOURCE_MODEL)
                .stackId(StackId)
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .logicalResourceIdentifier("logicalId").build();
        try {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);
        } catch (CfnInvalidRequestException e) {
            assertThat(e.getMessage()).isEqualTo("Invalid request provided: Unmodifiable DB Parameter: param");
        }

        verify(proxyRdsClient.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(proxyRdsClient.client()).describeDBClusterParametersPaginator(any(DescribeDbClusterParametersRequest.class));
    }

    @Test
    public void handleRequest_SimpleInProgressFailedUnsupportedParams() {
        final CreateHandler handler = new CreateHandler();
        final CreateDbClusterParameterGroupResponse createDbClusterParameterGroupResponse = CreateDbClusterParameterGroupResponse.builder().build();
        when(rds.createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class))).thenReturn(createDbClusterParameterGroupResponse);
        mockDescribeDbClusterParametersResponse("static", "dynamic", true);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .desiredResourceState(RESOURCE_MODEL)
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .stackId(StackId)
                .logicalResourceIdentifier("logicalId").build();
        request.getDesiredResourceState().setParameters(Collections.singletonMap("Wrong Key", "Wrong value"));

        try {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);
        } catch (CfnInvalidRequestException e) {
            assertThat(e.getMessage()).isEqualTo("Invalid request provided: Invalid / Unsupported DB Parameter: param");
        }

        verify(proxyRdsClient.client()).createDBClusterParameterGroup(any(CreateDbClusterParameterGroupRequest.class));
        verify(proxyRdsClient.client()).describeDBClusterParametersPaginator(any(DescribeDbClusterParametersRequest.class));
    }

    private void mockDescribeDbClusterParametersResponse(String firstParamApplyType,
                                                         String secondParamApplyType,
                                                         boolean isModifiable) {
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

        if (!isModifiable)
            return;

        final DescribeDbClusterParametersResponse describeDbClusterParametersResponse = DescribeDbClusterParametersResponse.builder().marker(null)
                .parameters(param1, param2).build();

        final DescribeDBClusterParametersIterable describeDbClusterParametersIterable = mock(DescribeDBClusterParametersIterable.class);
        when(describeDbClusterParametersIterable.stream())
                .thenReturn(Stream.<DescribeDbClusterParametersResponse>builder()
                        .add(describeDbClusterParametersResponse)
                        .build()
                );
        when(proxyRdsClient.client().describeDBClusterParametersPaginator(any(DescribeDbClusterParametersRequest.class))).thenReturn(describeDbClusterParametersIterable);
    }
}
