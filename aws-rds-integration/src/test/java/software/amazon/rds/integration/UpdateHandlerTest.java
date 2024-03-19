package software.amazon.rds.integration;

import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsRequest;
import software.amazon.awssdk.services.rds.model.Integration;
import software.amazon.awssdk.services.rds.model.IntegrationAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.IntegrationConflictOperationException;
import software.amazon.awssdk.services.rds.model.InvalidIntegrationStateException;
import software.amazon.awssdk.services.rds.model.ModifyIntegrationRequest;
import software.amazon.awssdk.services.rds.model.ModifyIntegrationResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.test.common.core.HandlerName;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static software.amazon.rds.integration.BaseHandlerStd.CALLBACK_DELAY;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractHandlerTest {

    private static final String RESOURCE_UPDATED_AT = "resource-updated-at";

    private static final String RETRIABLE_CONFLICT_OPERATION_MESSAGE = "Cannot modify because " +
            "another operation is in progress for the " +
            "Amazon Redshift data warehouse specified by the Amazon Resource Name (ARN). " +
            "Try again after the current operation completes.";

    private static final String RETRIABLE_INVALID_STATE_MESSAGE = "Unable to modify" +
            " because it is not in a valid state. Wait until the integration is in a valid state and try again.";

    @Mock
    RdsClient rdsClient;

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Getter
    private UpdateHandler handler;

    private boolean expectServiceInvocation;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.UPDATE;
    }

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler(TEST_HANDLER_CONFIG);
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
    void handleRequest_Success() {
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().modifyIntegration(any(ModifyIntegrationRequest.class)))
                .thenReturn(ModifyIntegrationResponse.builder().build());

        Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_ACTIVE);

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceState(Translator.translateToModel(INTEGRATION_ACTIVE))
                        .previousResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                        .desiredResourceState(Translator.translateToModel(INTEGRATION_ACTIVE.toBuilder()
                                .description(DESCRIPTION_ALTER)
                                .dataFilter(DATA_FILTER_ALTER)
                                .integrationName(INTEGRATION_NAME_ALTER)
                                .build()))
                        .desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST_ALTER)),
                () -> Optional.ofNullable(transitions.poll())
                        .orElse(INTEGRATION_ACTIVE
                                .toBuilder()
                                .tags(toAPITags(TAG_LIST_ALTER))
                                .description(DESCRIPTION_ALTER)
                                .dataFilter(DATA_FILTER_ALTER)
                                .integrationName(INTEGRATION_NAME_ALTER)
                                .build()),
                () -> INTEGRATION_ACTIVE_MODEL,
                () -> INTEGRATION_ACTIVE_MODEL.toBuilder()
                        .tags(TAG_LIST_ALTER)
                        .description(DESCRIPTION_ALTER)
                        .dataFilter(DATA_FILTER_ALTER)
                        .integrationName(INTEGRATION_NAME_ALTER)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).describeIntegrations(any(DescribeIntegrationsRequest.class));
        verify(rdsProxy.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client(), times(1)).modifyIntegration(any(ModifyIntegrationRequest.class));
    }

    @Test
    void handleRequest_ifBusyWithInvalidIntegrationStateException_shouldRetry() {
        when(rdsProxy.client().modifyIntegration(any(ModifyIntegrationRequest.class)))
                .thenThrow(InvalidIntegrationStateException.builder().message(RETRIABLE_INVALID_STATE_MESSAGE).build());

        Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_ACTIVE);

        test_handleRequest_base(
                new CallbackContext(),
                // resource handler builder
                ResourceHandlerRequest.<ResourceModel>builder(),
                // resource supplier - nobody calls describe
                null,
                // previous state
                () -> INTEGRATION_ACTIVE_MODEL,
                // desired  state
                () -> INTEGRATION_ACTIVE_MODEL.toBuilder()
                        .dataFilter(DATA_FILTER_ALTER)
                        .build(),
                // expect
                expectInProgress(CALLBACK_DELAY)
        );

        verify(rdsProxy.client(), times(1))
                .modifyIntegration(ArgumentMatchers.<ModifyIntegrationRequest>argThat((req) ->
                        Objects.equals(req.dataFilter(), DATA_FILTER_ALTER) &&
                        Objects.equals(req.integrationIdentifier(), INTEGRATION_ARN) &&
                        Objects.isNull(req.description()) &&
                        Objects.isNull(req.integrationName()))
                );
    }

    @Test
    void handleRequest_ifBusyWithIntegrationConflictOperationException_shouldRetry() {
        when(rdsProxy.client().modifyIntegration(any(ModifyIntegrationRequest.class)))
                .thenThrow(IntegrationConflictOperationException.builder().message(RETRIABLE_CONFLICT_OPERATION_MESSAGE).build());

        Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_ACTIVE);

        test_handleRequest_base(
                new CallbackContext(),
                // resource handler builder
                ResourceHandlerRequest.<ResourceModel>builder(),
                // resource supplier - nobody calls describe
                null,
                // previous state
                () -> INTEGRATION_ACTIVE_MODEL,
                // desired  state
                () -> INTEGRATION_ACTIVE_MODEL.toBuilder()
                        .dataFilter(DATA_FILTER_ALTER)
                        .build(),
                // expect
                expectInProgress(CALLBACK_DELAY)
        );

        verify(rdsProxy.client(), times(1))
                .modifyIntegration(ArgumentMatchers.<ModifyIntegrationRequest>argThat((req) ->
                        Objects.equals(req.dataFilter(), DATA_FILTER_ALTER) &&
                                Objects.equals(req.integrationIdentifier(), INTEGRATION_ARN) &&
                                Objects.isNull(req.description()) &&
                                Objects.isNull(req.integrationName()))
                );
    }

    @Test
    void handleRequest_partial_modify_only_description_should_only_modify_description() {
        when(rdsProxy.client().modifyIntegration(any(ModifyIntegrationRequest.class)))
                .thenReturn(ModifyIntegrationResponse.builder().build());

        Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_ACTIVE);

        test_handleRequest_base(
                new CallbackContext(),
                // resource handler builder
                ResourceHandlerRequest.<ResourceModel>builder(),
                // resource supplier
                () -> Optional.ofNullable(transitions.poll())
                        .orElse(INTEGRATION_ACTIVE
                                .toBuilder()
                                .description(DESCRIPTION_ALTER)
                                .build()),
                // previous state
                () -> INTEGRATION_ACTIVE_MODEL,
                // desired  state
                () -> INTEGRATION_ACTIVE_MODEL.toBuilder()
                        .description(DESCRIPTION_ALTER)
                        .build(),
                // expect
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeIntegrations(any(DescribeIntegrationsRequest.class));
        verify(rdsProxy.client(), times(1))
                .modifyIntegration(ArgumentMatchers.<ModifyIntegrationRequest>argThat((req) ->
                        Objects.equals(req.description(), DESCRIPTION_ALTER) &&
                        Objects.equals(req.integrationIdentifier(), INTEGRATION_ARN) &&
                        Objects.isNull(req.dataFilter()) &&
                        Objects.isNull(req.integrationName()))
                );
    }

    @Test
    void handleRequest_partial_modify_duplicateName_shouldFailWithConflict() {
        when(rdsProxy.client().modifyIntegration(any(ModifyIntegrationRequest.class)))
                .thenThrow(IntegrationAlreadyExistsException.builder().message("duplicate name").build());

        Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_ACTIVE);

        test_handleRequest_base(
                new CallbackContext(),
                // resource handler builder
                ResourceHandlerRequest.<ResourceModel>builder(),
                // resource supplier - nobody calls describe
                null,
                // previous state
                () -> INTEGRATION_ACTIVE_MODEL,
                // desired  state
                () -> INTEGRATION_ACTIVE_MODEL.toBuilder()
                        .integrationName(INTEGRATION_NAME_ALTER)
                        .build(),
                // expect
                expectFailed(HandlerErrorCode.AlreadyExists)
        );

        verify(rdsProxy.client(), times(1))
                .modifyIntegration(ArgumentMatchers.<ModifyIntegrationRequest>argThat((req) ->
                        Objects.equals(req.integrationName(), INTEGRATION_NAME_ALTER) &&
                        Objects.equals(req.integrationIdentifier(), INTEGRATION_ARN) &&
                        Objects.isNull(req.dataFilter()) &&
                        Objects.isNull(req.description()))
                );
    }

    @Test
    void handleRequest_partial_modify_withDescriptionGoingToEmpty_shouldNotModify() {
        Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_ACTIVE);

        test_handleRequest_base(
                new CallbackContext(),
                // resource handler builder
                ResourceHandlerRequest.<ResourceModel>builder(),
                // resource supplier
                () -> Optional.ofNullable(transitions.poll())
                        .orElse(INTEGRATION_ACTIVE),
                // previous state
                () -> INTEGRATION_ACTIVE_MODEL,
                // desired  state
                () -> INTEGRATION_ACTIVE_MODEL.toBuilder()
                        .description("")
                        .build(),
                // expect
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeIntegrations(any(DescribeIntegrationsRequest.class));
    }

    @Test
    void handleRequest_partial_modify_withIntegrationNameGoingToEmpty_shouldNotGenerateANewName() {
        Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_ACTIVE);

        test_handleRequest_base(
                new CallbackContext(),
                // resource handler builder
                ResourceHandlerRequest.<ResourceModel>builder(),
                // resource supplier
                () -> Optional.ofNullable(transitions.poll())
                        .orElse(INTEGRATION_ACTIVE),
                // previous state
                () -> INTEGRATION_ACTIVE_MODEL,
                // desired  state
                () -> INTEGRATION_ACTIVE_MODEL.toBuilder()
                        .integrationName(null)
                        .build(),
                // expect
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeIntegrations(any(DescribeIntegrationsRequest.class));
    }

    @Test
    void handleRequest_partial_modify_withNoIntegrationName_shouldNotChangeIntegrationName() {
        Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_ACTIVE);

        test_handleRequest_base(
                new CallbackContext(),
                // resource handler builder
                ResourceHandlerRequest.<ResourceModel>builder(),
                // resource supplier
                () -> Optional.ofNullable(transitions.poll())
                        .orElse(INTEGRATION_ACTIVE),
                // previous state
                () -> INTEGRATION_MODEL_WITH_NO_NAME,
                // desired  state
                () -> INTEGRATION_MODEL_WITH_NO_NAME.toBuilder()
                        .description("differentdescription") // only description changed
                        .build(),
                // expect
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeIntegrations(any(DescribeIntegrationsRequest.class));
        verify(rdsProxy.client(), times(1))
                .modifyIntegration(ArgumentMatchers.<ModifyIntegrationRequest>argThat((req) ->
                        Objects.equals("differentdescription", req.description()) &&
                        Objects.equals(req.integrationIdentifier(), INTEGRATION_ARN) &&
                        Objects.isNull(req.dataFilter()) &&
                        Objects.isNull(req.integrationName()))
                );
    }

}
