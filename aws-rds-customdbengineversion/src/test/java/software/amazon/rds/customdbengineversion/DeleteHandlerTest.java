package software.amazon.rds.customdbengineversion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CustomDbEngineVersionNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteCustomDbEngineVersionRequest;
import software.amazon.awssdk.services.rds.model.DeleteCustomDbEngineVersionResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsRequest;
import software.amazon.awssdk.services.rds.model.InvalidCustomDbEngineVersionStateException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    RdsClient rdsClient;

    @Getter
    private DeleteHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.DELETE;
    }

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler(HandlerConfig.builder().backoff(TEST_BACKOFF_DELAY).build());
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        rdsProxy = mockProxy(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        when(rdsProxy.client().deleteCustomDBEngineVersion(any(DeleteCustomDbEngineVersionRequest.class)))
                .thenReturn(DeleteCustomDbEngineVersionResponse.builder().build());
        when(rdsProxy.client().describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class)))
                .thenThrow(CustomDbEngineVersionNotFoundException.builder().message(MSG_NOT_FOUND).build());


        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        assertThat(response.getMessage()).isNull();

        verify(rdsProxy.client(), times(1)).deleteCustomDBEngineVersion(any(DeleteCustomDbEngineVersionRequest.class));
    }

    @Test
    public void handleRequest_NotFound() {
        when(rdsProxy.client().deleteCustomDBEngineVersion(any(DeleteCustomDbEngineVersionRequest.class)))
                .thenThrow(CustomDbEngineVersionNotFoundException.builder().message(MSG_NOT_FOUND).build());

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client(), times(1)).deleteCustomDBEngineVersion(any(DeleteCustomDbEngineVersionRequest.class));
    }


    @Test
    public void handleRequest_CevIsBeingDeleted() {
        when(rdsProxy.client().deleteCustomDBEngineVersion(any(DeleteCustomDbEngineVersionRequest.class))).thenThrow(
                InvalidCustomDbEngineVersionStateException.builder()
                        .message("Custom DB Engine Version 19.unit-test is already being deleted.")
                        .build());
        when(rdsProxy.client().describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class)))
                .thenThrow(CustomDbEngineVersionNotFoundException.builder().message(MSG_NOT_FOUND).build());


        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        assertThat(response.getMessage()).isNull();

        verify(rdsProxy.client(), times(1)).deleteCustomDBEngineVersion(any(DeleteCustomDbEngineVersionRequest.class));
    }

    @Test
    public void handleRequest_IsDeleting_Stabilize() {

        final DeleteCustomDbEngineVersionResponse deleteCustomDbEngineVersionResponse = DeleteCustomDbEngineVersionResponse.builder().build();
        when(rdsProxy.client().deleteCustomDBEngineVersion(any(DeleteCustomDbEngineVersionRequest.class))).thenReturn(deleteCustomDbEngineVersionResponse);

        AtomicBoolean fetchedOnce = new AtomicBoolean(false);
        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (fetchedOnce.compareAndSet(false, true)) {
                        return DB_ENGINE_VERSION_DELETING;
                    }
                    throw CustomDbEngineVersionNotFoundException.builder().build();
                },
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST).build(),
                expectSuccess()
        );

        assertThat(response.getMessage()).isNull();

        verify(rdsProxy.client(), times(1)).deleteCustomDBEngineVersion(any(DeleteCustomDbEngineVersionRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class));
    }
}
