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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import jdk.nashorn.internal.runtime.regexp.joni.exception.InternalException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.cloudformation.proxy.*;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;

import software.amazon.awssdk.services.rds.model.CreateOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateOptionGroupResponse;
import software.amazon.rds.common.handler.Tagging;

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
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_CreateSuccess() {
        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenReturn(CreateOptionGroupResponse.builder().build());

        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(randomString(32, ALPHA))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId(randomString(32, ALPHA))
                .logicalResourceIdentifier(randomString(32, ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_CreateSuccess_NoOptionGroupName() {
        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenReturn(CreateOptionGroupResponse.builder().build());

        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final String clientRequestToken = randomString(32, ALPHA);
        final String stackId = randomString(32, ALPHA);
        final String logicalResourceIdentifier = randomString(32, ALPHA);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(clientRequestToken)
                .desiredResourceState(RESOURCE_MODEL_NO_OPTION_GROUP_NAME)
                .stackId(stackId)
                .logicalResourceIdentifier(logicalResourceIdentifier)
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_CreateSuccess_RequiresModify() {
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
                .clientRequestToken(randomString(32, ALPHA))
                .desiredResourceState(RESOURCE_MODEL_WITH_CONFIGURATIONS)
                .stackId(randomString(32, ALPHA))
                .logicalResourceIdentifier(randomString(32, ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_SoftFailingTagging() {
        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenReturn(CreateOptionGroupResponse.builder().build());

        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build())
                .thenReturn(AddTagsToResourceResponse.builder().build());

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(randomString(32, ALPHA))
                .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags())))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId(randomString(32, ALPHA))
                .logicalResourceIdentifier(randomString(32, ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));

        ArgumentCaptor<CreateOptionGroupRequest> createOptionGroupCaptor = ArgumentCaptor.forClass(CreateOptionGroupRequest.class);
        verify(proxyClient.client(), times(1)).createOptionGroup(createOptionGroupCaptor.capture());
        final CreateOptionGroupRequest createOptionGroupWithSystemTags = createOptionGroupCaptor.getAllValues().get(0);
        Assertions.assertThat(createOptionGroupWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class)
        );

        ArgumentCaptor<AddTagsToResourceRequest> addTagsCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(proxyClient.client(), times(1)).addTagsToResource(addTagsCaptor.capture());
        final AddTagsToResourceRequest requestWithAllTags = addTagsCaptor.getAllValues().get(0);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getStackTags(), software.amazon.awssdk.services.rds.model.Tag.class)
        );
    }

    @Test
    public void handleRequest_HardFailingTagging() {
        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenReturn(CreateOptionGroupResponse.builder().build());

        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(randomString(32, ALPHA))
                .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                .desiredResourceState(RESOURCE_MODEL_WITH_RESOURCE_TAGS)
                .stackId(randomString(32, ALPHA))
                .logicalResourceIdentifier(randomString(32, ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        Assertions.assertThat(response).isNotNull();
        Assertions.assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        Assertions.assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        Assertions.assertThat(response.getMessage()).isNotNull();
        Assertions.assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AccessDenied);
        Assertions.assertThat(response.getResourceModels()).isNull();

        verify(proxyClient.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client(), times(1)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));

        ArgumentCaptor<CreateOptionGroupRequest> createOptionGroupCaptor = ArgumentCaptor.forClass(CreateOptionGroupRequest.class);
        verify(proxyClient.client(), times(1)).createOptionGroup(createOptionGroupCaptor.capture());
        final CreateOptionGroupRequest createOptionGroupWithSystemTags = createOptionGroupCaptor.getAllValues().get(0);
        Assertions.assertThat(createOptionGroupWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class)
        );

        ArgumentCaptor<AddTagsToResourceRequest> addTagsCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(proxyClient.client(), times(1)).addTagsToResource(addTagsCaptor.capture());
        final AddTagsToResourceRequest requestWithAllTags = addTagsCaptor.getAllValues().get(0);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getResourceTags(), software.amazon.awssdk.services.rds.model.Tag.class)
        );
    }

    @Test
    public void handleRequest_AlreadyExists() {
        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenThrow(OptionGroupAlreadyExistsException.class);

        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(randomString(32, ALPHA))
                .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId(randomString(32, ALPHA))
                .logicalResourceIdentifier(randomString(32, ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_RuntimeException() {
        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class)))
                .thenThrow(new RuntimeException("test exception"));

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(randomString(32, ALPHA))
                .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                .desiredResourceState(RESOURCE_MODEL)
                .stackId(randomString(32, ALPHA))
                .logicalResourceIdentifier(randomString(32, ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        Assertions.assertThat(response).isNotNull();
        Assertions.assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        Assertions.assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        Assertions.assertThat(response.getMessage()).isNotNull();
        Assertions.assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
        Assertions.assertThat(response.getResourceModels()).isNull();

        verify(proxyClient.client(), times(1)).createOptionGroup(any(CreateOptionGroupRequest.class));
    }
}
