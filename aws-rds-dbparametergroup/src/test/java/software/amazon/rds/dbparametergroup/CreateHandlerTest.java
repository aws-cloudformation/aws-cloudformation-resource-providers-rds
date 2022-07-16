package software.amazon.rds.dbparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.Collections;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateDbParameterGroupResponse;
import software.amazon.awssdk.services.rds.model.DBParameterGroup;
import software.amazon.awssdk.services.rds.model.DbParameterGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbParameterGroupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeEngineDefaultParametersRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.ModifyDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbParameterGroupResponse;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {
    private final static String VALIDATION_ONLY = "validation-only";
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
    public void tear_down(TestInfo testInfo) {
        if (!testInfo.getTags().contains(VALIDATION_ONLY)) {
            verify(rdsClient, atLeastOnce()).serviceName();
        }
        verifyNoMoreInteractions(rdsClient);

    }

    @Test
    public void handleRequest_SimpleSuccessWithoutApplyParameters() {
        mockDescribeDBParameterGroup();

        CallbackContext context = new CallbackContext();
        context.setParameterGroupCreated(true);
        context.setAddTagsComplete(true);
        context.setParametersApplied(true);

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

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, context, proxyClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client()).describeDBParameterGroups(any(DescribeDbParameterGroupsRequest.class));
        verify(proxyClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));

    }

    @Test
    public void handleRequest_CreateDbParameterGroupIfNotCreated() {
        mockCreateCall();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(RESET_RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackDelaySeconds()).isNotZero();

        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        verify(rdsClient).createDBParameterGroup(any(CreateDbParameterGroupRequest.class));
    }

    @Test
    public void handleRequest_DescribeEngineDefaultParameters() {
        final CallbackContext context = new CallbackContext();
        context.setParameterGroupCreated(true);
        context.setAddTagsComplete(true);

        mockDescribeEngineDefaultParametersResponse(proxyClient, "static", "dynamic", true, false);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(RESET_RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, context, proxyClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackDelaySeconds()).isNotZero();

        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        assertThat(context.isDefaultParametersFetched()).isTrue();
        assertThat(context.getDefaultParametersMarker()).isNullOrEmpty();
        verify(proxyClient.client()).describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class));
    }

    @Test
    public void handleRequest_DescribeEngineDefaultParametersPaginated() {
        final CallbackContext context = new CallbackContext();
        context.setParameterGroupCreated(true);
        context.setAddTagsComplete(true);

        mockDescribeEngineDefaultParametersResponse(proxyClient, "static", "dynamic", true, true);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(RESET_RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, context, proxyClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.IN_PROGRESS);
        assertThat(response.getCallbackDelaySeconds()).isNotZero();

        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        assertThat(context.isDefaultParametersFetched()).isFalse();
        assertThat(context.getDefaultParametersMarker()).isEqualTo("marker");
        verify(proxyClient.client()).describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessWithApplyParameters() {
        mockDescribeDBParameterGroup();

        final ModifyDbParameterGroupResponse modifyDbParameterGroupResponse = ModifyDbParameterGroupResponse.builder().build();
        when(proxyClient.client().modifyDBParameterGroup(any(ModifyDbParameterGroupRequest.class))).thenReturn(modifyDbParameterGroupResponse);

        final CallbackContext context = new CallbackContext();
        context.setParameterGroupCreated(true);
        context.setDefaultParametersFetched(true);
        context.setDefaultParameters(createParameterMap("static", "dynamic", true, true));

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(RESET_RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, context, proxyClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client()).modifyDBParameterGroup(any(ModifyDbParameterGroupRequest.class));
    }

    @Test
    public void handleRequest_SimpleFailWithAccessDenied() {
        final String message = "AccessDenied on create request";
        final CreateDbParameterGroupResponse createDbParameterGroupResponse = CreateDbParameterGroupResponse
                .builder().dbParameterGroup(DB_PARAMETER_GROUP_ACTIVE).build();
        when(rdsClient.createDBParameterGroup(any(CreateDbParameterGroupRequest.class)))
                .thenThrow(AwsServiceException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder().errorMessage(message).errorCode("AccessDenied").build())
                        .build());

        CallbackContext callbackContext = new CallbackContext();
        callbackContext.setParametersApplied(true);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId("StackId")
                .logicalResourceIdentifier("logicalId").build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).contains(message);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AccessDenied);
    }

    @Test
    @Tag(value = VALIDATION_ONLY)
    public void handleRequest_SimpleUnmodifiableParameterFail() {
        final CallbackContext context = new CallbackContext();
        context.setParameterGroupCreated(true);
        context.setDefaultParametersFetched(true);
        context.setDefaultParameters(createParameterMap("static", "dynamic", false, true));

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();

        final ProgressEvent<ResourceModel, CallbackContext> progress = handler.handleRequest(proxy, request, context, proxyClient, EMPTY_REQUEST_LOGGER);
        assertThat(progress.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(progress.getMessage()).isEqualTo("Invalid / Unmodifiable / Unsupported DB Parameter: param1");
    }

    @Test
    @Tag(value = VALIDATION_ONLY)
    public void handleRequest_SimpleInProgressFailedUnsupportedParams() {
        final CallbackContext context = new CallbackContext();
        context.setParameterGroupCreated(true);
        context.setDefaultParametersFetched(true);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();
        final ProgressEvent<ResourceModel, CallbackContext> progress = handler.handleRequest(proxy, request, context, proxyClient, EMPTY_REQUEST_LOGGER);

        assertThat(progress.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(progress.getMessage()).isEqualTo("Invalid / Unmodifiable / Unsupported DB Parameter: param1");

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
    public void handleRequest_SimpleThrottlingException() {
        when(rdsClient.describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class)))
                .thenThrow(RdsException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("ThrottlingException").build())
                        .build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(getClientRequestToken())
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER).build();

        final CallbackContext context =  new CallbackContext();
        context.setParameterGroupCreated(true);
        context.setAddTagsComplete(true);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, context, proxyClient, EMPTY_REQUEST_LOGGER);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.Throttling);
        verify(rdsClient).describeEngineDefaultParameters(any(DescribeEngineDefaultParametersRequest.class));
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

    private void mockCreateCall() {
        createDbParameterGroupResponse = CreateDbParameterGroupResponse.builder().dbParameterGroup(DBParameterGroup.builder().dbParameterGroupArn("arn").build()).build();
        when(proxyClient.client().createDBParameterGroup(any(CreateDbParameterGroupRequest.class))).thenReturn(createDbParameterGroupResponse);
    }
}
