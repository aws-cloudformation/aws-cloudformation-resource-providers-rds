package software.amazon.rds.dbsubnetgroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.CreateDbParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateDbParameterGroupResponse;
import software.amazon.awssdk.services.rds.model.CreateDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateDbSubnetGroupResponse;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.awssdk.services.rds.model.DbSubnetGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsResponse;
import software.amazon.awssdk.services.rds.model.InvalidSubnetException;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    RdsClient rds;
    @Mock
    private AmazonWebServicesClientProxy proxy;
    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;
    private CreateHandler handler;

    @BeforeEach
    public void setup() {
        handler = new CreateHandler(HandlerConfig.builder()
                .probingEnabled(false)
                .backoff(TEST_BACKOFF_DELAY)
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
    public void handleRequest_SimpleSuccess() {
        mockCreateCall();
        final DescribeDbSubnetGroupsResponse describeCreatingDbSubnetGroupsResponse = DescribeDbSubnetGroupsResponse.builder().dbSubnetGroups(DB_SUBNET_GROUP_CREATING).build();
        final DescribeDbSubnetGroupsResponse describeActiveDbSubnetGroupsResponse = DescribeDbSubnetGroupsResponse.builder().dbSubnetGroups(DB_SUBNET_GROUP_ACTIVE).build();

        AtomicInteger attempt = new AtomicInteger(2);
        when(proxyRdsClient.client().describeDBSubnetGroups(any(DescribeDbSubnetGroupsRequest.class))).then((m) -> {
            switch (attempt.getAndDecrement()) {
                case 2:
                    return describeCreatingDbSubnetGroupsResponse;
                default:
                    return describeActiveDbSubnetGroupsResponse;
            }
        });
        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyRdsClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier("dbsubnet")
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .clientRequestToken("4b90a7e4-b791-4512-a137-0cf12a23451e")
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);


        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createDBSubnetGroup(any(CreateDbSubnetGroupRequest.class));
        verify(proxyRdsClient.client(), times(3)).describeDBSubnetGroups(any(DescribeDbSubnetGroupsRequest.class));
        verify(proxyRdsClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }


    @Test
    public void handleRequest_SimpleSuccessAlternative() {
        mockCreateCall();
        final DescribeDbSubnetGroupsResponse describeCreatingDbSubnetGroupsResponse = DescribeDbSubnetGroupsResponse.builder().dbSubnetGroups(DB_SUBNET_GROUP_CREATING).build();
        final DescribeDbSubnetGroupsResponse describeActiveDbSubnetGroupsResponse = DescribeDbSubnetGroupsResponse.builder().dbSubnetGroups(DB_SUBNET_GROUP_ACTIVE).build();

        AtomicInteger attempt = new AtomicInteger(2);
        when(proxyRdsClient.client().describeDBSubnetGroups(any(DescribeDbSubnetGroupsRequest.class))).then((m) -> {
            switch (attempt.getAndDecrement()) {
                case 2:
                    return describeCreatingDbSubnetGroupsResponse;
                default:
                    return describeActiveDbSubnetGroupsResponse;
            }
        });
        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyRdsClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_ALTERNATIVE)
                .logicalResourceIdentifier("dbsubnet")
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .clientRequestToken("4b90a7e4-b791-4512-a137-0cf12a23451e")
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);


        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).createDBSubnetGroup(any(CreateDbSubnetGroupRequest.class));
        verify(proxyRdsClient.client(), times(3)).describeDBSubnetGroups(any(DescribeDbSubnetGroupsRequest.class));
        verify(proxyRdsClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessAlreadyExist() {

        when(proxyRdsClient.client().createDBSubnetGroup(any(CreateDbSubnetGroupRequest.class))).thenThrow(
                DbSubnetGroupAlreadyExistsException.class
        );

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_ALTERNATIVE)
                .logicalResourceIdentifier("dbsubnet")
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .clientRequestToken("4b90a7e4-b791-4512-a137-0cf12a23451e")
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);


        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AlreadyExists);

        verify(proxyRdsClient.client()).createDBSubnetGroup(any(CreateDbSubnetGroupRequest.class));
    }

    @Test
    public void handleRequest_SimpleException() {
        when(proxyRdsClient.client().createDBSubnetGroup(any(CreateDbSubnetGroupRequest.class))).thenThrow(
                InvalidParameterException.class
        );

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);


        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);

        verify(proxyRdsClient.client()).createDBSubnetGroup(any(CreateDbSubnetGroupRequest.class));
    }

    @Test
    public void handleRequest_SimpleInvalidSubnetException() {
        when(proxyRdsClient.client().createDBSubnetGroup(any(CreateDbSubnetGroupRequest.class))).thenThrow(
                InvalidSubnetException.class
        );

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);


        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackContext()).isNotNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InvalidRequest);

        verify(proxyRdsClient.client()).createDBSubnetGroup(any(CreateDbSubnetGroupRequest.class));
    }

    @Test
    public void handleRequest_SimpleFailWithAccessDenied() {
        final String message = "AccessDenied on create request";
        when(rds.createDBSubnetGroup(any(CreateDbSubnetGroupRequest.class)))
                .thenThrow(AwsServiceException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder().errorMessage(message).errorCode("AccessDenied").build())
                        .build());

        CallbackContext callbackContext = new CallbackContext();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken("token")
                .desiredResourceTags(translateTagsToMap(TAG_SET))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId("StackId")
                .logicalResourceIdentifier("logicalId").build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).contains(message);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AccessDenied);
    }

    private void mockCreateCall() {
        final CreateDbSubnetGroupResponse createDbSubnetGroupResponse = CreateDbSubnetGroupResponse.builder().dbSubnetGroup(DBSubnetGroup.builder().dbSubnetGroupArn("arn").build()).build();
        when(proxyRdsClient.client().createDBSubnetGroup(any(CreateDbSubnetGroupRequest.class))).thenReturn(createDbSubnetGroupResponse);
    }
}
