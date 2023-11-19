package software.amazon.rds.integration;

import lombok.Getter;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CreateIntegrationRequest;
import software.amazon.awssdk.services.rds.model.CreateIntegrationResponse;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsRequest;
import software.amazon.awssdk.services.rds.model.Integration;
import software.amazon.awssdk.services.rds.model.IntegrationAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.IntegrationConflictOperationException;
import software.amazon.cloudformation.exceptions.CfnNotStabilizedException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.test.common.core.HandlerName;

import java.time.Duration;
import java.util.Objects;
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
public class CreateHandlerTest extends AbstractHandlerTest {

    private final String DUPLICATE_INTEGRATION_ERROR_MESSAGE = "A zero-ETL integration named ct80e2519bfd4d4ad1bfdc943c66ce0482 " +
            "already exists in account 123123123123. Integration names must be unique within an account." +
            " Specify a different name for your integration, or delete the existing integration.";

    @Mock
    @Getter
    RdsClient rdsClient;

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Getter
    private CreateHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.CREATE;
    }

    @BeforeEach
    public void setup() {
        handler = new CreateHandler(TEST_HANDLER_CONFIG);
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }


    @Test
    public void handleRequest_CreateIntegration_withAllFields_success() {
        when(rdsProxy.client().createIntegration(any(CreateIntegrationRequest.class)))
                .thenReturn(CreateIntegrationResponse.builder()
                        .build());


        // Integration goes from CREATING -> ACTIVE, when everything is normal
        Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_CREATING);

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return INTEGRATION_ACTIVE;
                },
                () -> INTEGRATION_ACTIVE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createIntegration(
                ArgumentMatchers. <CreateIntegrationRequest>argThat(req -> {
                    // TODO verify the content
                   return true;
                })
        );
        verify(rdsProxy.client(), times(3)).describeIntegrations(
                ArgumentMatchers.<DescribeIntegrationsRequest>argThat(req ->
                    Objects.equals(INTEGRATION_CREATING.integrationArn(), req.integrationIdentifier())
                )
        );
    }

    @Test
    public void handleRequest_CreateIntegration_withNoName_shouldGenerateName() {
        when(rdsProxy.client().createIntegration(any(CreateIntegrationRequest.class)))
                .thenReturn(CreateIntegrationResponse.builder()
                        .build());

        // Integration goes from CREATING -> ACTIVE, when everything is normal
        Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_CREATING);

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (transitions.size() > 0) {
                        return transitions.remove();
                    }
                    return INTEGRATION_ACTIVE;
                },
                () -> INTEGRATION_MODEL_WITH_NO_NAME,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createIntegration(
                ArgumentMatchers. <CreateIntegrationRequest>argThat(req -> req.integrationName().contains(LOGICAL_RESOURCE_IDENTIFIER))
        );
        verify(rdsProxy.client(), times(3)).describeIntegrations(
                ArgumentMatchers.<DescribeIntegrationsRequest>argThat(req ->
                        Objects.equals(INTEGRATION_CREATING.integrationArn(), req.integrationIdentifier())
                )
        );
    }

    @Test
    public void handleRequest_CreateIntegration_withTerminalFailureState_returnFailure() {
        when(rdsProxy.client().createIntegration(any(CreateIntegrationRequest.class)))
                .thenReturn(CreateIntegrationResponse.builder()
                        .build());

        // Integration goes from CREATING -> ACTIVE, when everything is normal
        Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_CREATING);
        Assertions.assertThatThrownBy(() ->
                test_handleRequest_base(
                    new CallbackContext(),
                    () -> {
                        if (transitions.size() > 0) {
                            return transitions.remove();
                        }
                        return INTEGRATION_FAILED;
                    },
                    () -> INTEGRATION_ACTIVE_MODEL, // unused
                    expectFailed(HandlerErrorCode.NotStabilized) // unused
              )
        ).isInstanceOf(CfnNotStabilizedException.class);

        verify(rdsProxy.client(), times(1)).createIntegration(
                ArgumentMatchers. <CreateIntegrationRequest>argThat(req -> {
                    // TODO verify the content
                    return true;
                })
        );

        verify(rdsProxy.client(), times(2)).describeIntegrations(
                ArgumentMatchers.<DescribeIntegrationsRequest>argThat(req ->
                        Objects.equals(INTEGRATION_CREATING.integrationArn(), req.integrationIdentifier())
                )
        );
    }

    @Test
    public void handleRequest_CreateIntegration_withIntegrationAlreadyExistsException_returnFailure() {
        when(rdsProxy.client().createIntegration(any(CreateIntegrationRequest.class)))
                .thenThrow(IntegrationAlreadyExistsException.builder()
                        .message("Integration with the name already exists")
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> INTEGRATION_ACTIVE_MODEL,
                expectFailed(HandlerErrorCode.AlreadyExists)
        );

        verify(rdsProxy.client(), times(1)).createIntegration(
                ArgumentMatchers. <CreateIntegrationRequest>argThat(req -> {
                    // TODO verify the content
                    return true;
                })
        );

    }

    @Test
    public void handleRequest_CreateIntegration_withDuplicateIntegrationName_returnFailure() {
        when(rdsProxy.client().createIntegration(any(CreateIntegrationRequest.class)))
                .thenThrow(IntegrationConflictOperationException.builder()
                        .message(DUPLICATE_INTEGRATION_ERROR_MESSAGE)
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> INTEGRATION_ACTIVE_MODEL,
                expectFailed(HandlerErrorCode.AlreadyExists)
        );

        verify(rdsProxy.client(), times(1)).createIntegration(
                ArgumentMatchers. <CreateIntegrationRequest>argThat(req -> {
                    // TODO verify the content
                    return true;
                })
        );

    }
}
