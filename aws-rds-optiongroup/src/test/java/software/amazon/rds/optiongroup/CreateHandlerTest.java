package software.amazon.rds.optiongroup;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateOptionGroupResponse;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.ModifyOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.ModifyOptionGroupResponse;
import software.amazon.awssdk.services.rds.model.Option;
import software.amazon.awssdk.services.rds.model.OptionGroup;
import software.amazon.awssdk.services.rds.model.OptionSetting;
import software.amazon.awssdk.services.rds.model.Tag;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient sdkClient;

    @BeforeEach
    public void setup() {
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        sdkClient = mock(RdsClient.class);
        proxyClient = MOCK_PROXY(proxy, sdkClient);
    }

    @AfterEach
    public void post_execute() {
        verifyNoMoreInteractions(proxyClient.client());
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final CreateHandler handler = new CreateHandler();

        final CreateOptionGroupResponse createOptionGroupResponse = CreateOptionGroupResponse.builder().build();
        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class))).thenReturn(createOptionGroupResponse);

        final ModifyOptionGroupResponse modifyOptionGroupResponse = ModifyOptionGroupResponse.builder().build();
        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class))).thenReturn(modifyOptionGroupResponse);

        final DescribeOptionGroupsResponse describeOptionGroupsResponse = DescribeOptionGroupsResponse.builder()
            .optionGroupsList(
                OptionGroup.builder()
                    .build()
            )
            .build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeOptionGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final ResourceModel model = ResourceModel.builder()
            .engineName("sampleEngine")
            .majorEngineVersion("sampleMajorVersion")
            .optionGroupDescription("sampleDescription")
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .logicalResourceIdentifier("sampleResource")
            .clientRequestToken("sampleToken")
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client()).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client()).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client()).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessWithoutOptionSetting() {
        final CreateHandler handler = new CreateHandler();

        final CreateOptionGroupResponse createOptionGroupResponse = CreateOptionGroupResponse.builder().build();
        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class))).thenReturn(createOptionGroupResponse);

        final ModifyOptionGroupResponse modifyOptionGroupResponse = ModifyOptionGroupResponse.builder().build();
        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class))).thenReturn(modifyOptionGroupResponse);

        final DescribeOptionGroupsResponse describeOptionGroupsResponse = DescribeOptionGroupsResponse.builder()
            .optionGroupsList(
                OptionGroup.builder()
                    .build()
            )
            .build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeOptionGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder().build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final ResourceModel model = ResourceModel.builder()
            .engineName("sampleEngine")
            .majorEngineVersion("sampleMajorVersion")
            .optionGroupDescription("sampleDescription")
            .optionConfigurations(Sets.newHashSet(OptionConfiguration.builder().build()))
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .logicalResourceIdentifier("sampleResource")
            .clientRequestToken("sampleToken")
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client()).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client()).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client()).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_SimpleSuccessWithId() {
        final CreateHandler handler = new CreateHandler();

        final CreateOptionGroupResponse createOptionGroupResponse = CreateOptionGroupResponse.builder().build();
        when(proxyClient.client().createOptionGroup(any(CreateOptionGroupRequest.class))).thenReturn(createOptionGroupResponse);

        final ModifyOptionGroupResponse modifyOptionGroupResponse = ModifyOptionGroupResponse.builder().build();
        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class))).thenReturn(modifyOptionGroupResponse);

        final DescribeOptionGroupsResponse describeOptionGroupsResponse = DescribeOptionGroupsResponse.builder()
            .optionGroupsList(
                OptionGroup.builder()
                    .engineName("sampleEngine")
                    .options(
                        Option.builder()
                            .optionSettings(OptionSetting.builder().build())
                        .build())
                    .build()
            )
            .build();
        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class))).thenReturn(describeOptionGroupsResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
            .tagList(Tag.builder().key("sampleKey").value("sampleValue").build())
            .build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class))).thenReturn(listTagsForResourceResponse);

        final ResourceModel model = ResourceModel.builder()
            .id("sampleId")
            .engineName("sampleEngine")
            .majorEngineVersion("sampleMajorVersion")
            .optionGroupDescription("sampleDescription")
            .optionConfigurations(Sets.newHashSet(
                OptionConfiguration.builder()
                    .optionSettings(Sets.newHashSet(software.amazon.rds.optiongroup.OptionSetting.builder().build()))
                .build()
            ))
            .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
            .desiredResourceState(model)
            .desiredResourceTags(ImmutableMap.of("sampleKey", "sampleValue"))
            .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client()).createOptionGroup(any(CreateOptionGroupRequest.class));
        verify(proxyClient.client()).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client()).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client()).listTagsForResource(any(ListTagsForResourceRequest.class));
    }
}
