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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DeleteOptionGroupRequest;
import software.amazon.awssdk.services.rds.model.DeleteOptionGroupResponse;
import software.amazon.awssdk.services.rds.model.OptionGroupNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.HandlerConfig;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    @Getter
    private DeleteHandler handler;

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler(HandlerConfig.builder()
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
    public void handleRequest_DeleteSuccess() {
        when(proxyClient.client().deleteOptionGroup(any(DeleteOptionGroupRequest.class)))
                .thenReturn(DeleteOptionGroupResponse.builder().build());

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        assertThat(response.getMessage()).isNull();

        verify(proxyClient.client(), times(1)).deleteOptionGroup(any(DeleteOptionGroupRequest.class));
    }

    @Test
    public void handleRequest_NotFound() {
        when(proxyClient.client().deleteOptionGroup(any(DeleteOptionGroupRequest.class)))
                .thenThrow(OptionGroupNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(proxyClient.client(), times(1)).deleteOptionGroup(any(DeleteOptionGroupRequest.class));
    }

    @Test
    public void handleRequest_RuntimeException() {
        when(proxyClient.client().deleteOptionGroup(any(DeleteOptionGroupRequest.class)))
                .thenThrow(new RuntimeException("test exception"));

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(proxyClient.client(), times(1)).deleteOptionGroup(any(DeleteOptionGroupRequest.class));
    }
}
