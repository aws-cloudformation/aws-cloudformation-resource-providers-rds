package software.amazon.rds.optiongroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;

import com.google.common.collect.Iterables;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateOptionGroupResponse;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.ModifyOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.ModifyOptionGroupResponse;
import software.amazon.awssdk.services.rds.model.OptionGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.TestUtils;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    @Getter
    private CreateHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.CREATE;
    }

    @BeforeEach
    public void setup() {
        handler = new CreateHandler(
                HandlerConfig.builder()
                        .backoff(TEST_BACKOFF_DELAY)
                        .build()
        );
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyClient = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down(TestInfo testInfo) {
        if (testInfo.getTags().contains(IGNORE_TEST_VERIFICATION) == true) {
            return;
        }

        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_CreateSuccess() {
        ResourceModel RESOURCE_MODEL = RESOURCE_MODEL_BUILDER().build();

        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenReturn(CreateOptionGroupResponse.builder().build());

        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, new CallbackContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client(), times(1)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_timestamp() {
        ResourceModel RESOURCE_MODEL = RESOURCE_MODEL_BUILDER().build();

        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenReturn(CreateOptionGroupResponse.builder().build());

        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();

        final CallbackContext callbackContext = new CallbackContext();
        Instant start = Instant.ofEpochSecond(0);
        callbackContext.timestampOnce("START", start);
        Duration timeToAdd = Duration.ofSeconds(50);
        Instant end = Instant.ofEpochSecond(0).plus(timeToAdd);
        callbackContext.timestamp("END", start);
        callbackContext.timestamp("END", end);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, callbackContext);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client(), times(1)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        assertThat(callbackContext.getTimestamp("START").isBefore(Instant.now())).isTrue();
        assertThat(callbackContext.getTimestamp("START")).isEqualTo(start);
        assertThat(callbackContext.getTimestamp("END")).isEqualTo(end);
    }

    @Test
    public void handleRequest_timeDelta() {
        ResourceModel RESOURCE_MODEL = RESOURCE_MODEL_BUILDER().build();

        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenReturn(CreateOptionGroupResponse.builder().build());

        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();

        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.calculateTimeDeltaInMinutes("TimeDeltaTest", Instant.ofEpochSecond(0), Instant.ofEpochSecond(60));


        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, callbackContext);


        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client(), times(1)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        assertThat(callbackContext.getTimeDelta().get("TimeDeltaTest")).isEqualTo(1.00);
    }

    @Test
    public void handleRequest_WithOptionGroupName_CreatesOptionGroup() {
        ResourceModel RESOURCE_MODEL_WITH_NAME = RESOURCE_MODEL_WITH_NAME_BUILDER().build();

        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenReturn(CreateOptionGroupResponse.builder().build());

        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .desiredResourceState(RESOURCE_MODEL_WITH_NAME)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, new CallbackContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        ArgumentCaptor<CreateOptionGroupRequest> createOptionGroupCaptor = ArgumentCaptor.forClass(CreateOptionGroupRequest.class);

        verify(proxyClient.client(), times(1)).createOptionGroup(createOptionGroupCaptor.capture());
        assertThat(createOptionGroupCaptor.getValue().optionGroupName()).isEqualTo("OptionGroupName");
        verify(proxyClient.client(), times(1)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }


    @Test
    public void handleRequest_CreateSuccess_RequiresModify() {
        ResourceModel RESOURCE_MODEL_WITH_CONFIGURATIONS = RESOURCE_MODEL_WITH_CONFIGURATIONS_BUILDER().build();

        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenReturn(CreateOptionGroupResponse.builder().build());

        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class)))
                .thenReturn(ModifyOptionGroupResponse.builder().build());

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .desiredResourceState(RESOURCE_MODEL_WITH_CONFIGURATIONS)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, new CallbackContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client(), times(1)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_SoftFailingTagging() {
        ResourceModel RESOURCE_MODEL = RESOURCE_MODEL_BUILDER().build();

        final Tagging.TagSet desiredTags = Tagging.TagSet.builder()
                .systemTags(TAG_SET.getSystemTags())
                .stackTags(TAG_SET.getStackTags())
                .build();

        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .message("Role not authorized to execute rds:AddTagsToResource")
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(desiredTags.getSystemTags())))
                .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(desiredTags.getStackTags())))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, new CallbackContext());


        Assertions.assertThat(response).isNotNull();
        Assertions.assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        Assertions.assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        Assertions.assertThat(response.getMessage()).isNotNull();
        Assertions.assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.UnauthorizedTaggingOperation);
        Assertions.assertThat(response.getResourceModels()).isNull();

        ArgumentCaptor<CreateOptionGroupRequest> createOptionGroupCaptor = ArgumentCaptor.forClass(CreateOptionGroupRequest.class);
        verify(proxyClient.client(), times(1)).createOptionGroup(createOptionGroupCaptor.capture());
        final CreateOptionGroupRequest createOptionGroupWithAllTags = createOptionGroupCaptor.getAllValues().get(0);
        Assertions.assertThat(createOptionGroupWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(desiredTags), software.amazon.awssdk.services.rds.model.Tag.class)
        );
    }

    @Test
    public void handleRequest_HardFailingTagging() {
        ResourceModel RESOURCE_MODEL_WITH_RESOURCE_TAGS = RESOURCE_MODEL_WITH_RESOURCE_TAGS_BUILDER().build();

        final Tagging.TagSet desiredTags = Tagging.TagSet.builder()
                .systemTags(TAG_SET.getSystemTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                .desiredResourceState(RESOURCE_MODEL_WITH_RESOURCE_TAGS)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, new CallbackContext());

        Assertions.assertThat(response).isNotNull();
        Assertions.assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        Assertions.assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        Assertions.assertThat(response.getMessage()).isNotNull();
        Assertions.assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AccessDenied);
        Assertions.assertThat(response.getResourceModels()).isNull();

        ArgumentCaptor<CreateOptionGroupRequest> createOptionGroupCaptor = ArgumentCaptor.forClass(CreateOptionGroupRequest.class);
        verify(proxyClient.client(), times(1)).createOptionGroup(createOptionGroupCaptor.capture());
        final CreateOptionGroupRequest createOptionGroupWithAllTags = createOptionGroupCaptor.getAllValues().get(0);
        Assertions.assertThat(createOptionGroupWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(desiredTags), software.amazon.awssdk.services.rds.model.Tag.class)
        );
    }

    @Test
    public void handleRequest_AlreadyExists() {
        ResourceModel RESOURCE_MODEL = RESOURCE_MODEL_BUILDER().build();

        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenThrow(OptionGroupAlreadyExistsException.class);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, new CallbackContext());

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AlreadyExists);

        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
    }

    @Test
    public void handleRequest_RuntimeException() {
        ResourceModel RESOURCE_MODEL = RESOURCE_MODEL_BUILDER().build();

        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenThrow(new RuntimeException("test exception"));

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, proxyClient, request, new CallbackContext());

        Assertions.assertThat(response).isNotNull();
        Assertions.assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        Assertions.assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        Assertions.assertThat(response.getMessage()).isNotNull();
        Assertions.assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
        Assertions.assertThat(response.getResourceModels()).isNull();

        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
    }
}
