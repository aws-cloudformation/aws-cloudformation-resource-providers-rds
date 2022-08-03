package software.amazon.rds.dbparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParametersResponse;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultParametersRequest;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultParametersResponse;
import software.amazon.awssdk.services.rds.model.EngineDefaults;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbParameterGroupResponse;
import software.amazon.awssdk.services.rds.model.Parameter;
import software.amazon.awssdk.services.rds.model.ResetDbParameterGroupRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    @Mock
    RdsClient rdsClient;
    @Captor
    ArgumentCaptor<ResetDbParameterGroupRequest> captor;
    @Mock
    private AmazonWebServicesClientProxy proxy;
    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;
    private DBParameterGroup simpleDbParameterGroup;
    private ResourceModel previousResourceModel;

    private ResourceHandlerRequest<ResourceModel> sameParamsRequest;
    private ResourceHandlerRequest<ResourceModel> updateParamsRequest;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyRdsClient = MOCK_PROXY(proxy, rdsClient);

        simpleDbParameterGroup = DBParameterGroup.builder()
                .dbParameterGroupArn("arn").build();

        previousResourceModel = ResourceModel.builder()
                .parameters(null)
                .build();

        sameParamsRequest = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(RESOURCE_MODEL)
                .previousResourceState(RESOURCE_MODEL)
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .build();

        updateParamsRequest = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(RESOURCE_MODEL)
                .previousResourceState(previousResourceModel)
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .build();
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final UpdateHandler handler = new UpdateHandler();

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        final DescribeDbParameterGroupsResponse describeDbParameterGroupsResponse = DescribeDbParameterGroupsResponse.builder()
                .dbParameterGroups(simpleDbParameterGroup).build();
        when(rdsClient.describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class))).thenReturn(describeDbParameterGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rdsClient.addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, updateParamsRequest, callbackContext, proxyRdsClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client(), times(1)).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
        verify(proxyRdsClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyRdsClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessWithApplyParameters() {
        final UpdateHandler handler = new UpdateHandler();

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        final DescribeDbParameterGroupsResponse describeDbParameterGroupsResponse = DescribeDbParameterGroupsResponse.builder()
                .dbParameterGroups(simpleDbParameterGroup).build();
        when(rdsClient.describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class))).thenReturn(describeDbParameterGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        mockDescribeDbParametersResponse(proxyRdsClient, "static", "dynamic", true, true, false);

        final ModifyDbParameterGroupResponse modifyDbParameterGroupResponse = ModifyDbParameterGroupResponse.builder().build();
        when(rdsClient.modifyDBParameterGroup(any(ModifyDbParameterGroupRequest.class))).thenReturn(modifyDbParameterGroupResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .previousResourceState(previousResourceModel)
                .desiredResourceState(RESET_RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(rdsClient).resetDBParameterGroup(captor.capture());
        assertThat(captor.getValue().parameters().get(0).applyMethod().toString().equals("pending-reboot")).isTrue();
        verify(rdsClient).describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class));
    }

    private Parameter foo(int i) {
        return (Parameter.builder()
                .parameterName(String.format("param%d", i+1))
                .parameterValue("default_value")
                .isModifiable(true)
                .applyType("dynamic")
                .applyMethod("immediate")
                .build());
    }

    @Test
    public void handleRequest_SimpleSuccessWithManyApplyParameters() {
        final UpdateHandler handler = new UpdateHandler();
        Map<String, Object > resetParameters = new HashMap<>();
        resetParameters.put("tx_isolation", "value");

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        final DescribeDbParameterGroupsResponse describeDbParameterGroupsResponse = DescribeDbParameterGroupsResponse.builder()
                .dbParameterGroups(simpleDbParameterGroup).build();
        when(rdsClient.describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class))).thenReturn(describeDbParameterGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final DescribeEngineDefaultParametersResponse describeEngineDefaultParametersResponse = DescribeEngineDefaultParametersResponse.builder()
                .engineDefaults(EngineDefaults.builder()
                                .parameters(MANY_DEFAULT_PARAMETERS_SORTED)
                                .build()
                ).build();
        when(proxyRdsClient.client().describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class))).thenReturn(describeEngineDefaultParametersResponse);

        final DescribeDbParametersResponse describeDbParametersResponse = DescribeDbParametersResponse.builder().marker(null)
                .parameters(MANY_CURRENT_PARAMETERS_SORTED).build();
        when(proxyRdsClient.client().describeDBParameters(any(DescribeDbParametersRequest.class))).thenReturn(describeDbParametersResponse);

        final ModifyDbParameterGroupResponse modifyDbParameterGroupResponse = ModifyDbParameterGroupResponse.builder().build();
        when(rdsClient.modifyDBParameterGroup(any(ModifyDbParameterGroupRequest.class))).
                thenReturn(modifyDbParameterGroupResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .previousResourceState(previousResourceModel)
                .desiredResourceState(RESET_RESOURCE_MODEL.toBuilder().parameters(resetParameters).build())
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(rdsClient, times(1)).describeDBParameters(any(DescribeDbParametersRequest.class));
        verify(rdsClient, times(1)).describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class));
        verify(rdsClient, times(1)).modifyDBParameterGroup(any(ModifyDbParameterGroupRequest.class));
        verify(proxyRdsClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(rdsClient).describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class));
        verify(rdsClient, times(2)).resetDBParameterGroup(captor.capture());

        assertThat(captor.getAllValues().get(0).parameters().equals(MANY_DEFAULT_PARAMETERS_SORTED.subList(0, 20))).isTrue();
        assertThat(captor.getAllValues().get(1).parameters().equals(MANY_DEFAULT_PARAMETERS_SORTED.subList(20, 25))).isTrue();
    }

    @Test
    public void handleRequest_SimpleSuccessWithApplyParametersPaginated() {
        final UpdateHandler handler = new UpdateHandler();

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        final DescribeDbParameterGroupsResponse describeDbParameterGroupsResponse = DescribeDbParameterGroupsResponse.builder()
                .dbParameterGroups(simpleDbParameterGroup).build();
        when(rdsClient.describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class))).thenReturn(describeDbParameterGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        mockDescribeDbParametersResponse(proxyRdsClient, "static", "dynamic", true, true, true);

        final ModifyDbParameterGroupResponse modifyDbParameterGroupResponse = ModifyDbParameterGroupResponse.builder().build();
        when(rdsClient.modifyDBParameterGroup(any(ModifyDbParameterGroupRequest.class))).thenReturn(modifyDbParameterGroupResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .previousResourceState(previousResourceModel)
                .desiredResourceState(RESET_RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(rdsClient).resetDBParameterGroup(captor.capture());
        assertThat(captor.getValue().parameters().get(0).applyMethod().toString().equals("pending-reboot")).isTrue();
        verify(rdsClient, times(2)).describeDBParameters(any(DescribeDbParametersRequest.class));
        verify(rdsClient, times(2)).describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessSameParams() {
        final UpdateHandler handler = new UpdateHandler();

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        final DescribeDbParameterGroupsResponse describeDbParameterGroupsResponse = DescribeDbParameterGroupsResponse.builder()
                .dbParameterGroups(simpleDbParameterGroup).build();
        when(rdsClient.describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class))).thenReturn(describeDbParameterGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(rdsClient.listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rdsClient.addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, sameParamsRequest, callbackContext, proxyRdsClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client(), times(1)).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
        verify(proxyRdsClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyRdsClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
    }
}
