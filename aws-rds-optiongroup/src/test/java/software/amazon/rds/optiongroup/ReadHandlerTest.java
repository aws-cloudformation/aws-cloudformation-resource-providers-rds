package software.amazon.rds.optiongroup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeOptionGroupsRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.HandlerConfig;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends TestCommon {

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

    @BeforeEach
    public void setup() {
        handler = new ReadHandler(HandlerConfig.builder()
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
}
