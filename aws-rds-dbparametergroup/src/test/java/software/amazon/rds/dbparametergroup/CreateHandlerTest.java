package software.amazon.rds.dbparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.Collections;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.CreateDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateDbParameterGroupResponse;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.awssdk.services.rds.model.DbParameterGroupAlreadyExistsException;
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
import software.amazon.awssdk.services.rds.paginators.DescribeDBParametersIterable;
import software.amazon.awssdk.services.rds.paginators.DescribeEngineDefaultParametersIterable;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    private CreateHandler handler;
    private CreateDbParameterGroupResponse createDbParameterGroupResponse;

    @BeforeEach
    public void setup() {
        handler = new CreateHandler();
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyClient = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccessWithoutApplyParameters() {
        mockCreateCall();
        mockDescribeDBParameterGroup();

        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(rdsClient.addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        ResourceModel EMPTY_NAME_RESOURCE_MODEL = ResourceModel.builder()
                .dBParameterGroupName("")
                .description("test DB Parameter group description")
                .family("testFamily")
                .tags(Collections.emptyList())
                .parameters(PARAMS)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .stackId("stackId")
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(EMPTY_NAME_RESOURCE_MODEL)
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client()).createDBParameterGroup(any(CreateDbParameterGroupRequest.class));
        verify(proxyClient.client()).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
        verify(proxyClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));

    }

    @Test
    public void handleRequest_SimpleSuccessWithApplyParameters() {
        mockCreateCall();
        mockDescribeDbParametersResponse("static", "dynamic", true);

        mockDescribeDBParameterGroup();


        final ModifyDbParameterGroupResponse modifyDbParameterGroupResponse = ModifyDbParameterGroupResponse.builder().build();
        when(proxyClient.client().modifyDBParameterGroup(any(ModifyDbParameterGroupRequest.class))).thenReturn(modifyDbParameterGroupResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(rdsClient).createDBParameterGroup(any(CreateDbParameterGroupRequest.class));
        verify(rdsClient).describeDBParametersPaginator(any(DescribeDbParametersRequest.class));
        verify(rdsClient).describeEngineDefaultParametersPaginator(any(DescribeEngineDefaultParametersRequest.class));
    }

    @Test
    public void handleRequest_SimpleUnmodifiableParameterFail() {
        mockCreateCall();
        mockDescribeDbParametersResponse("static", "dynamic", false);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();
        try {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, EMPTY_REQUEST_LOGGER);
        } catch (CfnInvalidRequestException e) {
            assertThat(e.getMessage()).isEqualTo("Invalid request provided: Unmodifiable DB Parameter: param1");
        }

        verify(proxyClient.client()).createDBParameterGroup(any(CreateDbParameterGroupRequest.class));
    }

    @Test
    public void handleRequest_SimpleInProgressFailedUnsupportedParams() {
        mockCreateCall();
        DescribeEngineDefaultParametersIterable describeDbParametersIterable = mock(DescribeEngineDefaultParametersIterable.class);
        when(rdsClient.describeEngineDefaultParametersPaginator(any(DescribeEngineDefaultParametersRequest.class))).thenReturn(describeDbParametersIterable);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();
        try {
            handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, EMPTY_REQUEST_LOGGER);
        } catch (CfnInvalidRequestException e) {
            assertThat(e.getMessage()).isEqualTo("Invalid request provided: Invalid / Unsupported DB Parameter: param1");
        }

        verify(proxyClient.client()).createDBParameterGroup(any(CreateDbParameterGroupRequest.class));
        verify(rdsClient).describeEngineDefaultParametersPaginator(any(DescribeEngineDefaultParametersRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessAlreadyExists() {
        when(proxyClient.client().createDBParameterGroup(any(CreateDbParameterGroupRequest.class))).thenThrow(
                DbParameterGroupAlreadyExistsException.class
        );

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AlreadyExists);

        verify(proxyClient.client()).createDBParameterGroup(any(CreateDbParameterGroupRequest.class));
    }

    @Test
    public void handleRequest_SimpleException() {
        when(proxyClient.client().createDBParameterGroup(any(CreateDbParameterGroupRequest.class)))
                .thenThrow(InvalidParameterException.class);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);

        verify(proxyClient.client()).createDBParameterGroup(any(CreateDbParameterGroupRequest.class));
    }

    private void mockDescribeDBParameterGroup() {
        final DescribeDbParameterGroupsResponse describeDbParameterGroupsResponse = DescribeDbParameterGroupsResponse.builder().dbParameterGroups(DB_PARAMETER_GROUP_ACTIVE).build();
        when(proxyClient.client().describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class))).thenReturn(describeDbParameterGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);
    }

    private void mockDescribeDbParametersResponse(String firstParamApplyType,
                                                  String secondParamApplyType,
                                                  boolean isModifiable) {
        Parameter param1 = Parameter.builder()
                .parameterName("param1")
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
        //Adding parameter to current parameters and not adding it to default. Expected behaviour is to ignore it
        Parameter param3 = Parameter.builder()
                .parameterName("param3")
                .parameterValue("system_value")
                .isModifiable(isModifiable)
                .applyType(secondParamApplyType)
                .build();


        DescribeEngineDefaultParametersIterable describeEngineDefaultParametersResponses = mock(DescribeEngineDefaultParametersIterable.class);
        final DescribeEngineDefaultParametersResponse describeEngineDefaultParametersResponse = DescribeEngineDefaultParametersResponse.builder()
                .engineDefaults(EngineDefaults.builder()
                        .parameters(param1, param2)
                        .build()
                ).build();
        when(describeEngineDefaultParametersResponses.stream())
                .thenReturn(Stream.<DescribeEngineDefaultParametersResponse>builder().
                        add(describeEngineDefaultParametersResponse)
                        .build()
                );
        when(proxyClient.client().describeEngineDefaultParametersPaginator(any(DescribeEngineDefaultParametersRequest.class))).thenReturn(describeEngineDefaultParametersResponses);

        if (!isModifiable)
            return;

        final DescribeDbParametersResponse describeDbParametersResponse = DescribeDbParametersResponse.builder().marker(null)
                .parameters(param1, param2, param3).build();

        final DescribeDBParametersIterable describeDbParametersIterable = mock(DescribeDBParametersIterable.class);
        when(describeDbParametersIterable.stream())
                .thenReturn(Stream.<DescribeDbParametersResponse>builder()
                        .add(describeDbParametersResponse)
                        .build()
                );
        when(proxyClient.client().describeDBParametersPaginator(any(DescribeDbParametersRequest.class))).thenReturn(describeDbParametersIterable);
    }

    private void mockCreateCall() {
        createDbParameterGroupResponse = CreateDbParameterGroupResponse.builder().dbParameterGroup(DBParameterGroup.builder().dbParameterGroupArn("arn").build()).build();
        when(proxyClient.client().createDBParameterGroup(any(CreateDbParameterGroupRequest.class))).thenReturn(createDbParameterGroupResponse);
    }
}
