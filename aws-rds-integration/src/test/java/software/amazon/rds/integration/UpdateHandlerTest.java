package software.amazon.rds.integration;

import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsRequest;
import software.amazon.awssdk.services.rds.model.Integration;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.test.common.core.HandlerName;

import java.time.Duration;
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

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractHandlerTest {

    private static final String RESOURCE_UPDATED_AT = "resource-updated-at";

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

        Queue<Integration> transitions = new ConcurrentLinkedQueue<>();
        transitions.add(INTEGRATION_ACTIVE);

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                        .desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST_ALTER)),
                () -> Optional.ofNullable(transitions.poll())
                        .orElse(INTEGRATION_ACTIVE
                                .toBuilder()
                                .tags(toAPITags(TAG_LIST_ALTER))
                                .build()),
                () -> INTEGRATION_ACTIVE_MODEL,
                () -> INTEGRATION_ACTIVE_MODEL.toBuilder()
                        .tags(TAG_LIST_ALTER)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(2)).describeIntegrations(any(DescribeIntegrationsRequest.class));
        verify(rdsProxy.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

}
