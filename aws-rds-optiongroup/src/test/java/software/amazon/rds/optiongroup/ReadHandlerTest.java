package software.amazon.rds.optiongroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.Option;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.OptionSetting;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.BaseProxyClient;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractTestBase {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    @Getter
    private ReadHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.READ;
    }

    @BeforeEach
    public void setup() {
        handler = new ReadHandler(HandlerConfig.builder()
                .backoff(TEST_BACKOFF_DELAY)
                .build());
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyClient = new BaseProxyClient<>(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_ReadSuccess() {
        ResourceModel RESOURCE_MODEL_WITH_NAME = RESOURCE_MODEL_WITH_NAME_BUILDER().build();

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> OPTION_GROUP_ACTIVE,
                () -> RESOURCE_MODEL_WITH_NAME,
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));
    }

    @Test
    public void handleRequest_NotFound() {
        ResourceModel RESOURCE_MODEL_WITH_NAME = RESOURCE_MODEL_WITH_NAME_BUILDER().build();

        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class)))
                .thenThrow(OptionGroupNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_WITH_NAME,
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(proxyClient.client(), times(1)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
    }

    @Test
    public void handleRequest_RuntimeException() {
        ResourceModel RESOURCE_MODEL_WITH_NAME = RESOURCE_MODEL_WITH_NAME_BUILDER().build();

        when(proxyClient.client().describeOptionGroups(any(DescribeOptionGroupsRequest.class)))
                .thenThrow(new RuntimeException("test exception"));

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_WITH_NAME,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(proxyClient.client(), times(1)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
    }

    @Test
    public void handleRequest_FilterDefinedOptions() {
        ResourceModel RESOURCE_MODEL_WITH_NAME = RESOURCE_MODEL_WITH_NAME_BUILDER()
                .build();

        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(ListTagsForResourceResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> result = test_handleRequest_base(
                new CallbackContext(),
                () -> OPTION_GROUP_ACTIVE.toBuilder()
                        .options(Option.builder()
                                        .optionName("option-name-1")
                                        .optionSettings(ImmutableList.of(
                                                OptionSetting.builder()
                                                        .name("option-setting-1")
                                                        .value("option-setting-value-1")
                                                        .defaultValue("option-setting-default-value-1")
                                                        .build(),
                                                OptionSetting.builder()
                                                        .name("option-setting-2")
                                                        .value("option-setting-default-value-2")
                                                        .defaultValue("option-setting-default-value-2")
                                                        .build(),
                                                OptionSetting.builder()
                                                        .name("option-setting-3")
                                                        .value("option-setting-value-3")
                                                        .defaultValue(null)
                                                        .build()
                                        ))
                                        .build(),
                                Option.builder()
                                        .optionName("option-name-2")
                                        .optionSettings(ImmutableList.of(
                                                OptionSetting.builder()
                                                        .name("option-setting-4")
                                                        .value("option-setting-default-value-4")
                                                        .defaultValue("option-setting-default-value-4")
                                                        .build()
                                        ))
                                        .build()
                        ).build(),
                () -> RESOURCE_MODEL_WITH_NAME,
                expectSuccess()
        );

        verify(proxyClient.client(), times(1)).describeOptionGroups(any(DescribeOptionGroupsRequest.class));
        verify(proxyClient.client(), times(1)).listTagsForResource(any(ListTagsForResourceRequest.class));

        final List<software.amazon.rds.optiongroup.OptionConfiguration> optionSettings = result.getResourceModel().getOptionConfigurations();
        assertThat(optionSettings).isEqualTo(ImmutableList.of(
                software.amazon.rds.optiongroup.OptionConfiguration.builder()
                        .optionName("option-name-1")
                        .optionSettings(ImmutableList.of(
                                software.amazon.rds.optiongroup.OptionSetting.builder()
                                        .name("option-setting-1")
                                        .value("option-setting-value-1")
                                        .build(),
                                software.amazon.rds.optiongroup.OptionSetting.builder()
                                        .name("option-setting-3")
                                        .value("option-setting-value-3")
                                        .build()
                        ))
                        .dBSecurityGroupMemberships(Collections.emptySet())
                        .vpcSecurityGroupMemberships(Collections.emptySet())
                        .build(),
                software.amazon.rds.optiongroup.OptionConfiguration.builder()
                        .optionName("option-name-2")
                        .optionSettings(Collections.emptyList())
                        .dBSecurityGroupMemberships(Collections.emptySet())
                        .vpcSecurityGroupMemberships(Collections.emptySet())
                        .build()
        ));
    }
}
