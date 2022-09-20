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
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.*;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.TestUtils;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    @Getter
    private UpdateHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.UPDATE;
    }

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler(HandlerConfig.builder()
                .backoff(TEST_BACKOFF_DELAY)
                .build());
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyClient = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_CoreUpdate_Success() {
        ResourceModel RESOURCE_MODEL_WITH_CONFIGURATIONS = RESOURCE_MODEL_WITH_CONFIGURATIONS_BUILDER().build();
        final ResourceModel RESOURCE_MODEL_WITH_UPDATED_CONFIGURATIONS = RESOURCE_MODEL_BUILDER()
                .optionConfigurations(ImmutableList.of(
                        OptionConfiguration.builder()
                                .optionName("testOptionConfiguration")
                                .optionVersion("2.2.3")
                                .build()
                )).build();

        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class)))
                .thenReturn(ModifyOptionGroupResponse.builder().build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .previousResourceState(RESOURCE_MODEL_WITH_CONFIGURATIONS)
                .desiredResourceState(RESOURCE_MODEL_WITH_UPDATED_CONFIGURATIONS)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_EmptyPreviousOptionVersion() {
        final ResourceModel RESOURCE_MODEL_WITH_CONFIGURATIONS = RESOURCE_MODEL_BUILDER()
                .optionConfigurations(ImmutableList.of(
                        OptionConfiguration.builder()
                                .optionName("APEX")
                                .optionVersion(null)
                                .build()
                )).build();
        final ResourceModel RESOURCE_MODEL_WITH_UPDATED_CONFIGURATIONS = RESOURCE_MODEL_BUILDER()
                .optionConfigurations(ImmutableList.of(
                        OptionConfiguration.builder()
                                .optionName("APEX")
                                .optionVersion("1.2.3")
                                .build()
                )).build();

        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class)))
                .thenReturn(ModifyOptionGroupResponse.builder().build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeDbClusterParameterGroupsResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .previousResourceState(RESOURCE_MODEL_WITH_CONFIGURATIONS)
                .desiredResourceState(RESOURCE_MODEL_WITH_UPDATED_CONFIGURATIONS)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_TagUpdate_Success() {
        ResourceModel RESOURCE_MODEL_WITH_RESOURCE_TAGS = RESOURCE_MODEL_WITH_RESOURCE_TAGS_BUILDER().build();
        ResourceModel RESOURCE_MODEL_WITH_UPDATED_RESOURCE_TAGS = RESOURCE_MODEL_BUILDER()
                .tags(
                    Translator.translateTagsFromSdk(Translator.translateTagsToSdk(
                        ImmutableMap.of("desiredKey", "desiredValue")
                    )
                )).build();


        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class)))
                .thenReturn(describeDbClusterParameterGroupsResponse);
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .previousResourceState(RESOURCE_MODEL_WITH_RESOURCE_TAGS)
                .desiredResourceState(RESOURCE_MODEL_WITH_UPDATED_RESOURCE_TAGS)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyClient.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_SoftFailingTaggingOnRemoveTags() {
        ResourceModel RESOURCE_MODEL_WITH_NAME = RESOURCE_MODEL_WITH_NAME_BUILDER().build();

        final Map<String, String> previousResourceTags = Translator.translateTagsToRequest(
                Translator.translateTagsFromSdk(
                        ImmutableSet.of(
                                Tag.builder().key("stack-tag-1").value("stack-tag-value1").build(),
                                Tag.builder().key("stack-tag-2").value("stack-tag-value2").build(),
                                Tag.builder().key("stack-tag-3").value("stack-tag-value3").build()
                        )
                )
        );

        final Map<String, String> desiredResourceTags = Translator.translateTagsToRequest(
                Translator.translateTagsFromSdk(
                        ImmutableSet.of(
                                Tag.builder().key("stack-tag-2").value("stack-tag-value2").build(),
                                Tag.builder().key("stack-tag-3").value("stack-tag-value3").build(),
                                Tag.builder().key("stack-tag-4").value("stack-tag-value4").build()
                        )
                )
        );

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class)))
                .thenReturn(describeDbClusterParameterGroupsResponse);
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .previousResourceTags(previousResourceTags)
                .desiredResourceTags(desiredResourceTags)
                .previousResourceState(RESOURCE_MODEL_WITH_NAME)
                .desiredResourceState(RESOURCE_MODEL_WITH_NAME)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_HardFailingTaggingOnAddTags() {
        ResourceModel RESOURCE_MODEL_WITH_NAME = RESOURCE_MODEL_WITH_NAME_BUILDER().build();
        ResourceModel RESOURCE_MODEL_WITH_RESOURCE_TAGS = RESOURCE_MODEL_WITH_RESOURCE_TAGS_BUILDER().build();

        final Map<String, String> previousResourceTags = Translator.translateTagsToRequest(
                Translator.translateTagsFromSdk(
                        ImmutableSet.of(
                                Tag.builder().key("stack-tag-1").value("stack-tag-value1").build(),
                                Tag.builder().key("stack-tag-2").value("stack-tag-value2").build()
                        )
                )
        );

        final Map<String, String> desiredResourceTags = Translator.translateTagsToRequest(
                Translator.translateTagsFromSdk(
                        ImmutableSet.of(
                                Tag.builder().key("stack-tag-1").value("stack-tag-value1").build(),
                                Tag.builder().key("stack-tag-2").value("stack-tag-value2").build(),
                                Tag.builder().key("stack-tag-3").value("stack-tag-value3").build()
                        )
                )
        );

        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class)))
                .thenReturn(describeDbClusterParameterGroupsResponse);
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .previousResourceState(RESOURCE_MODEL_WITH_NAME)
                .desiredResourceState(RESOURCE_MODEL_WITH_RESOURCE_TAGS)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getMessage()).isNotNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AccessDenied);
        assertThat(response.getResourceModels()).isNull();

        verify(proxyClient.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(proxyClient.client(), times(1)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
    }

    @Test
    public void handleRequest_VersionDowngrade_Success() {
        ResourceModel RESOURCE_MODEL_WITH_NAME = RESOURCE_MODEL_WITH_NAME_BUILDER().build();

        final ResourceModel previousModel = RESOURCE_MODEL_WITH_NAME.toBuilder()
                .optionConfigurations(Collections.singletonList(
                        OptionConfiguration.builder().optionName("test-option-name").optionVersion("1.2.3.4").build()
                ))
                .build();

        final ResourceModel desiredModel = RESOURCE_MODEL_WITH_NAME.toBuilder()
                .optionConfigurations(Collections.singletonList(
                        OptionConfiguration.builder().optionName("test-option-name").optionVersion("1.2.2.0").build()
                ))
                .build();

        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class)))
                .thenReturn(ModifyOptionGroupResponse.builder().build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class)))
                .thenReturn(describeDbClusterParameterGroupsResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .previousResourceState(previousModel)
                .desiredResourceState(desiredModel)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_VersionUpgrade_APEX_Success() {
        ResourceModel RESOURCE_MODEL_WITH_NAME = RESOURCE_MODEL_WITH_NAME_BUILDER().build();

        final ResourceModel previousModel = RESOURCE_MODEL_WITH_NAME.toBuilder()
                .optionConfigurations(Collections.singletonList(
                        OptionConfiguration.builder().optionName("APEX").optionVersion("1.2.3.4").build()
                ))
                .build();

        final ResourceModel desiredModel = RESOURCE_MODEL_WITH_NAME.toBuilder()
                .optionConfigurations(Collections.singletonList(
                        OptionConfiguration.builder().optionName("APEX").optionVersion("2.3.4").build()
                ))
                .build();

        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class)))
                .thenReturn(ModifyOptionGroupResponse.builder().build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class)))
                .thenReturn(describeDbClusterParameterGroupsResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .previousResourceState(previousModel)
                .desiredResourceState(desiredModel)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_VersionDowngrade_APEX_Success() {
        ResourceModel RESOURCE_MODEL_WITH_NAME = RESOURCE_MODEL_WITH_NAME_BUILDER().build();

        final ResourceModel previousModel = RESOURCE_MODEL_WITH_NAME.toBuilder()
                .optionConfigurations(Collections.singletonList(
                        OptionConfiguration.builder().optionName("APEX").optionVersion("1.2.3.4").build()
                ))
                .build();

        // Here we emulate a downgrade for an APEX option configuration.
        // UpdateHandler should detect the downgrade and restore the previous
        // version. Effectively this resolves in no change at all, therefore
        // we expect to UpdateOptionGroup call at all.
        final ResourceModel desiredModel = RESOURCE_MODEL_WITH_NAME.toBuilder()
                .optionConfigurations(Collections.singletonList(
                        OptionConfiguration.builder().optionName("APEX").optionVersion("1.2.2.0").build()
                ))
                .build();

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        final DescribeOptionGroupsResponse describeDbClusterParameterGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class)))
                .thenReturn(describeDbClusterParameterGroupsResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .previousResourceState(previousModel)
                .desiredResourceState(desiredModel)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(0)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_NotFound() {
        ResourceModel RESOURCE_MODEL_WITH_CONFIGURATIONS = RESOURCE_MODEL_WITH_CONFIGURATIONS_BUILDER().build();
        final ResourceModel previousModel = RESOURCE_MODEL_BUILDER()
                .optionConfigurations(Collections.emptyList())
                .build();

        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class)))
                .thenThrow(OptionGroupNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .previousResourceState(previousModel)
                .desiredResourceState(RESOURCE_MODEL_WITH_CONFIGURATIONS)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getMessage()).isNotNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
        assertThat(response.getResourceModels()).isNull();

        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
    }

    @Test
    public void handleRequest_RuntimeException() {
        ResourceModel RESOURCE_MODEL_WITH_CONFIGURATIONS = RESOURCE_MODEL_WITH_CONFIGURATIONS_BUILDER().build();

        final ResourceModel previousModel = RESOURCE_MODEL_WITH_CONFIGURATIONS.toBuilder()
                .optionConfigurations(Collections.emptyList())
                .build();

        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class)))
                .thenThrow(new RuntimeException("test exception"));

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .clientRequestToken(TestUtils.randomString(32, TestUtils.ALPHA))
                .previousResourceState(previousModel)
                .desiredResourceState(RESOURCE_MODEL_WITH_CONFIGURATIONS)
                .stackId(TestUtils.randomString(32, TestUtils.ALPHA))
                .logicalResourceIdentifier(TestUtils.randomString(32, TestUtils.ALPHA))
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getMessage()).isNotNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
        assertThat(response.getResourceModels()).isNull();

        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
    }
}
