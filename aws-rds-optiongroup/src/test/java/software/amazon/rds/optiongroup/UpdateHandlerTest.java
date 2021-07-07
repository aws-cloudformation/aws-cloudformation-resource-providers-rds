package software.amazon.rds.optiongroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.ModifyOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.ModifyOptionGroupResponse;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    protected static final String RESOURCE_IDENTIFIER = "option-group";
    protected static final String CLIENT_REQUEST_TOKEN = "3b8cacab-1328-456f-a11f-64efb80ab51a";

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    private UpdateHandler handler;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyClient = MOCK_PROXY(proxy, rdsClient);
        handler = new UpdateHandler();
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_Success_CoreUpdateOnly() {
        // We don't care about the values of the previous model,
        // we only ensure it's different from the desired one so
        // the core resource update triggers.
        final ResourceModel previousModel = ResourceModel.builder()
                .optionGroupName(RESOURCE_MODEL.getOptionGroupName())
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL;

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(previousModel)
                .desiredResourceState(desiredModel)
                .logicalResourceIdentifier(RESOURCE_IDENTIFIER)
                .clientRequestToken(CLIENT_REQUEST_TOKEN)
                .build();

        final ModifyOptionGroupResponse modifyOptionGroupResponse = ModifyOptionGroupResponse.builder().build();
        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class))).thenReturn(modifyOptionGroupResponse);

        final DescribeOptionGroupsResponse describeOptionGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeOptionGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);

        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client()).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_Success_TagUpdateOnly() {
        // In this case we initiate a no-core resource change request
        // and expect no modifyOptionGroup invocation at all.
        // The only change that should happen is delete-and-add tags.
        final ResourceModel previousModel = RESOURCE_MODEL;
        final ResourceModel desiredModel = RESOURCE_MODEL;

        final Map<String, String> previousTags = ImmutableMap.of(
                "foo", "bar",
                "boo", "baz"
        );

        final Map<String, String> desiredTags = ImmutableMap.of(
                "boo", "moo"
        );

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(previousModel)
                .desiredResourceState(desiredModel)
                .previousResourceTags(previousTags)
                .desiredResourceTags(desiredTags)
                .logicalResourceIdentifier(RESOURCE_IDENTIFIER)
                .clientRequestToken(CLIENT_REQUEST_TOKEN)
                .build();

        final DescribeOptionGroupsResponse describeOptionGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeOptionGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);

        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        // no modifyOptionGroup invocation is expected here
        verify(proxyClient.client(), times(0)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyClient.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_Success_OptionGroupVersionDowngrade_NoApex() {
        final ResourceModel previousModel = ResourceModel.builder()
                .optionGroupName(RESOURCE_IDENTIFIER)
                .optionGroupDescription("test option group description")
                .engineName("testEngineVersion")
                .majorEngineVersion("testMajorVersionName")
                .optionConfigurations(ImmutableList.of(
                        OptionConfiguration.builder()
                                .optionName("test-option-name")
                                .optionVersion("1.2.3.4")
                                .build()
                ))
                .build();


        final ResourceModel desiredModel = ResourceModel.builder()
                .optionGroupName(RESOURCE_IDENTIFIER)
                .optionGroupDescription("test option group description")
                .engineName("testEngineVersion")
                .majorEngineVersion("testMajorVersionName")
                .optionConfigurations(ImmutableList.of(
                        OptionConfiguration.builder()
                                .optionName("test-option-name")
                                .optionVersion("1.2.2.0")
                                .build()
                ))
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(previousModel)
                .desiredResourceState(desiredModel)
                .logicalResourceIdentifier(RESOURCE_IDENTIFIER)
                .clientRequestToken(CLIENT_REQUEST_TOKEN)
                .build();

        final ModifyOptionGroupResponse modifyOptionGroupResponse = ModifyOptionGroupResponse.builder().build();
        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class))).thenReturn(modifyOptionGroupResponse);

        final DescribeOptionGroupsResponse describeOptionGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeOptionGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);

        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client()).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_Success_OptionGroupVersionUpgrade_Apex() {
        final ResourceModel previousModel = ResourceModel.builder()
                .optionGroupName(RESOURCE_IDENTIFIER)
                .optionGroupDescription("test option group description")
                .engineName("testEngineVersion")
                .majorEngineVersion("testMajorVersionName")
                .optionConfigurations(ImmutableList.of(
                        OptionConfiguration.builder()
                                .optionName("APEX")
                                .optionVersion("1.2.3.4")
                                .build()
                ))
                .build();

        final ResourceModel desiredModel = ResourceModel.builder()
                .optionGroupName(RESOURCE_IDENTIFIER)
                .optionGroupDescription("test option group description")
                .engineName("testEngineVersion")
                .majorEngineVersion("testMajorVersionName")
                .optionConfigurations(ImmutableList.of(
                        OptionConfiguration.builder()
                                .optionName("APEX")
                                .optionVersion("2.3.4")
                                .build()
                ))
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(previousModel)
                .desiredResourceState(desiredModel)
                .logicalResourceIdentifier(RESOURCE_IDENTIFIER)
                .clientRequestToken(CLIENT_REQUEST_TOKEN)
                .build();

        final ModifyOptionGroupResponse modifyOptionGroupResponse = ModifyOptionGroupResponse.builder().build();
        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class))).thenReturn(modifyOptionGroupResponse);

        final DescribeOptionGroupsResponse describeOptionGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeOptionGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);

        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client()).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_Success_OptionGroupVersionDowngrade_Apex() {
        final ResourceModel previousModel = ResourceModel.builder()
                .optionGroupName(RESOURCE_IDENTIFIER)
                .optionGroupDescription("test option group description")
                .engineName("testEngineVersion")
                .majorEngineVersion("testMajorVersionName")
                .optionConfigurations(ImmutableList.of(
                        OptionConfiguration.builder()
                                .optionName("APEX")
                                .optionVersion("1.2.3.4")
                                .build()
                ))
                .build();

        // We emulate a downgrade behavior for an APEX option configuration.
        // UpdateHandler should detect the downgrade and restore the previous
        // version. Effectively this resolves in no change at all, therefore
        // we expect to UpdateOptionGroup call at all.
        final ResourceModel desiredModel = ResourceModel.builder()
                .optionGroupName(RESOURCE_IDENTIFIER)
                .optionGroupDescription("test option group description")
                .engineName("testEngineVersion")
                .majorEngineVersion("testMajorVersionName")
                .optionConfigurations(ImmutableList.of(
                        OptionConfiguration.builder()
                                .optionName("APEX")
                                .optionVersion("1.2.2.0")
                                .build()
                ))
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(previousModel)
                .desiredResourceState(desiredModel)
                .logicalResourceIdentifier(RESOURCE_IDENTIFIER)
                .clientRequestToken(CLIENT_REQUEST_TOKEN)
                .build();

        final DescribeOptionGroupsResponse describeOptionGroupsResponse = DescribeOptionGroupsResponse.builder()
                .optionGroupsList(OPTION_GROUP_ACTIVE).build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeOptionGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class))).thenReturn(removeTagsFromResourceResponse);

        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class))).thenReturn(addTagsToResourceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        // in this case we expect no ModifyOptionGroup call at all
        verify(proxyClient.client(), times(0)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_SimpleNotFound() {
        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class))).thenThrow(
                OptionGroupNotFoundException.class
        );

        final ResourceModel previousModel = ResourceModel.builder()
                .optionGroupName(RESOURCE_MODEL.getOptionGroupName())
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL;

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(previousModel)
                .desiredResourceState(desiredModel)
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);

        verify(proxyClient.client()).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
    }

    @Test
    public void handleRequest_SimpleException() {
        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class))).thenThrow(
                InvalidParameterException.class
        );

        final ResourceModel previousModel = ResourceModel.builder()
                .optionGroupName(RESOURCE_MODEL.getOptionGroupName())
                .build();
        final ResourceModel desiredModel = RESOURCE_MODEL;

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(previousModel)
                .desiredResourceState(desiredModel)
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);

        verify(proxyClient.client()).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
    }
}
