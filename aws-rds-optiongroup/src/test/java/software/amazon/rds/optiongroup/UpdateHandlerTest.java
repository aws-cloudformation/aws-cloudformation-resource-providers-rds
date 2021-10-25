package software.amazon.rds.optiongroup;

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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.ModifyOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.ModifyOptionGroupResponse;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.handler.HandlerConfig;

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
    }

    @Test
    public void handleRequest_CoreUpdate_Success() {
        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class)))
                .thenReturn(ModifyOptionGroupResponse.builder().build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> OPTION_GROUP_ACTIVE,
                () -> ResourceModel.builder().optionGroupName(RESOURCE_MODEL.getOptionGroupName()).build(),
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_TagUpdate_Success() {
        final Map<String, String> previousTags = ImmutableMap.of("foo", "bar", "boo", "baz");
        final Map<String, String> desiredTags = ImmutableMap.of("boo", "moo");

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceTags(previousTags)
                        .desiredResourceTags(desiredTags),
                () -> OPTION_GROUP_ACTIVE,
                () -> RESOURCE_MODEL,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
        verify(proxyClient.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(proxyClient.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_VersionDowngrade_Success() {
        final ResourceModel previousModel = RESOURCE_MODEL.toBuilder()
                .optionConfigurations(Collections.singletonList(
                        OptionConfiguration.builder().optionName("test-option-name").optionVersion("1.2.3.4").build()
                ))
                .build();

        final ResourceModel desiredModel = RESOURCE_MODEL.toBuilder()
                .optionConfigurations(Collections.singletonList(
                        OptionConfiguration.builder().optionName("test-option-name").optionVersion("1.2.2.0").build()
                ))
                .build();

        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class)))
                .thenReturn(ModifyOptionGroupResponse.builder().build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> OPTION_GROUP_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_VersionUpgrade_APEX_Success() {
        final ResourceModel previousModel = RESOURCE_MODEL.toBuilder()
                .optionConfigurations(Collections.singletonList(
                        OptionConfiguration.builder().optionName("APEX").optionVersion("1.2.3.4").build()
                ))
                .build();

        final ResourceModel desiredModel = RESOURCE_MODEL.toBuilder()
                .optionConfigurations(Collections.singletonList(
                        OptionConfiguration.builder().optionName("APEX").optionVersion("2.3.4").build()
                ))
                .build();

        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class)))
                .thenReturn(ModifyOptionGroupResponse.builder().build());
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> OPTION_GROUP_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_VersionDowngrade_APEX_Success() {
        final ResourceModel previousModel = RESOURCE_MODEL.toBuilder()
                .optionConfigurations(Collections.singletonList(
                        OptionConfiguration.builder().optionName("APEX").optionVersion("1.2.3.4").build()
                ))
                .build();

        // Here we emulate a downgrade for an APEX option configuration.
        // UpdateHandler should detect the downgrade and restore the previous
        // version. Effectively this resolves in no change at all, therefore
        // we expect to UpdateOptionGroup call at all.
        final ResourceModel desiredModel = RESOURCE_MODEL.toBuilder()
                .optionConfigurations(Collections.singletonList(
                        OptionConfiguration.builder().optionName("APEX").optionVersion("1.2.2.0").build()
                ))
                .build();

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> OPTION_GROUP_ACTIVE,
                () -> previousModel,
                () -> desiredModel,
                expectSuccess()
        );

        verify(proxyClient.client(), times(0)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
        verify(proxyClient.client(), times(2)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_NotFound() {
        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class)))
                .thenThrow(OptionGroupNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL.toBuilder()
                        .optionConfigurations(Collections.emptyList())
                        .build(),
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
    }

    @Test
    public void handleRequest_RuntimeException() {
        when(proxyClient.client().modifyOptionGroup(any(ModifyOptionGroupRequest.class)))
                .thenThrow(new RuntimeException("test exception"));

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL.toBuilder()
                        .optionConfigurations(Collections.emptyList())
                        .build(),
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(proxyClient.client(), times(1)).modifyOptionGroup(any(ModifyOptionGroupRequest.class));
    }
}
