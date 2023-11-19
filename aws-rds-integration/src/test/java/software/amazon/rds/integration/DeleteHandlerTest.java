package software.amazon.rds.integration;

import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DeleteIntegrationRequest;
import software.amazon.awssdk.services.rds.model.DeleteIntegrationResponse;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsRequest;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsResponse;
import software.amazon.awssdk.services.rds.model.Integration;
import software.amazon.awssdk.services.rds.model.IntegrationConflictOperationException;
import software.amazon.awssdk.services.rds.model.IntegrationNotFoundException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.test.common.core.HandlerName;

import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractHandlerTest {

    private static final String MSG_NOT_FOUND = "not found";

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    @Getter
    RdsClient rdsClient;

    @Getter
    private DeleteHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.DELETE;
    }

    private boolean expectServiceInvocation;

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler(TEST_HANDLER_CONFIG);
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = MOCK_PROXY(proxy, rdsClient);
        expectServiceInvocation = true;
    }

    @AfterEach
    public void tear_down() {
        if (expectServiceInvocation) {
            verify(rdsClient, atLeastOnce()).serviceName();
        }
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_deleting_should_fail_if_no_integration_found_before_first_call() {
        when(rdsProxy.client().describeIntegrations(any(DescribeIntegrationsRequest.class)))
                .then((a) -> {throw IntegrationNotFoundException.builder().message(MSG_NOT_FOUND).build();});


        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> INTEGRATION_ACTIVE_MODEL, // unused
                expectFailed(HandlerErrorCode.NotFound)
        );
    }

    @Test
    public void handleRequest_deleting_should_succeed() {
        when(rdsProxy.client().deleteIntegration(any(DeleteIntegrationRequest.class)))
                .thenReturn(DeleteIntegrationResponse.builder().build());

        final Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        // first call is the check whether the resource exists
        transitions.add(INTEGRATION_ACTIVE);
        transitions.add(INTEGRATION_DELETING);
        transitions.add(INTEGRATION_DELETING);

        when(rdsProxy.client().describeIntegrations(any(DescribeIntegrationsRequest.class)))
                .thenAnswer((a) -> {
                    if (transitions.size() > 0) {
                        return DescribeIntegrationsResponse.builder().integrations(transitions.remove()).build();
                    }
                    throw IntegrationNotFoundException.builder().message(MSG_NOT_FOUND).build();
                });

        ProgressEvent<ResourceModel, CallbackContext> progressEvent = ProgressEvent.<ResourceModel, CallbackContext>builder()
                .callbackContext(new CallbackContext()).build();

        final int DELAY = 6;
        while (progressEvent.getCallbackContext().getDeleteWaitTime() <= 500) {
            progressEvent = test_handleRequest_base(
                    progressEvent.getCallbackContext(),
                    null,
                    () -> INTEGRATION_ACTIVE_MODEL, // unused
                    expectInProgress(DELAY)
            );
        }
        test_handleRequest_base(
                progressEvent.getCallbackContext(),
                null,
                () -> INTEGRATION_ACTIVE_MODEL, // unused
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).deleteIntegration(any(DeleteIntegrationRequest.class));
    }

    @Test
    public void handleRequest_deleting_with_IntegrationNotFoundException_should_succeed() {
        when(rdsProxy.client().describeIntegrations(any(DescribeIntegrationsRequest.class)))
                .thenReturn(DescribeIntegrationsResponse.builder().integrations(INTEGRATION_ACTIVE).build());
        when(rdsProxy.client().deleteIntegration(any(DeleteIntegrationRequest.class)))
                .then((t) -> { throw IntegrationNotFoundException.builder().build(); });


        test_handleRequest_base(
                new CallbackContext(),
                null, // unused
                () -> INTEGRATION_ACTIVE_MODEL, // unused
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).deleteIntegration(any(DeleteIntegrationRequest.class));
    }

    @Test
    public void handleRequest_deleting_with_internalerror_should_fail() {
        when(rdsProxy.client().describeIntegrations(any(DescribeIntegrationsRequest.class)))
                .thenReturn(DescribeIntegrationsResponse.builder().integrations(INTEGRATION_ACTIVE).build());
        when(rdsProxy.client().deleteIntegration(any(DeleteIntegrationRequest.class)))
                .then((t) -> makeAwsServiceException(ErrorCode.InternalFailure));

        final Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_ACTIVE);
        transitions.add(INTEGRATION_DELETING);
        transitions.add(INTEGRATION_DELETING);

        test_handleRequest_base(
                new CallbackContext(),
                null, // unused
                () -> INTEGRATION_ACTIVE_MODEL, // unused
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client(), times(1)).deleteIntegration(any(DeleteIntegrationRequest.class));
    }

    @Test
    public void handleRequest_deleting_should_fail_when_IntegrationConflictOperationException() {
        when(rdsProxy.client().describeIntegrations(any(DescribeIntegrationsRequest.class)))
                .thenReturn(DescribeIntegrationsResponse.builder().integrations(INTEGRATION_ACTIVE).build());
        when(rdsProxy.client().deleteIntegration(any(DeleteIntegrationRequest.class)))
                .then((invocationOnMock) -> {
                    throw IntegrationConflictOperationException.builder()
                            .message("STILL CREATING")
                            .build();
                });

        final Queue<Integration> describeResponses = new ConcurrentLinkedQueue<>();
        // this is called when stabilizing after the first lambda call
        describeResponses.add(INTEGRATION_DELETING);

        test_handleRequest_base(
                new CallbackContext(),
                null, // unused
                () -> INTEGRATION_ACTIVE_MODEL, // unused
                expectFailed(HandlerErrorCode.ResourceConflict)
        );

        // this call will cause IntegrationConflictOperationException
        verify(rdsProxy.client(), times(1)).deleteIntegration(any(DeleteIntegrationRequest.class));
    }
}
