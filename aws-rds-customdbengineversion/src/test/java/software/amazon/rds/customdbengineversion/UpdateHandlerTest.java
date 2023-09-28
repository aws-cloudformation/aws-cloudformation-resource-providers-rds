package software.amazon.rds.customdbengineversion;

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
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.CustomDbEngineVersionNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsRequest;
import software.amazon.awssdk.services.rds.model.ModifyCustomDbEngineVersionRequest;
import software.amazon.awssdk.services.rds.model.ModifyCustomDbEngineVersionResponse;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    private RdsClient rdsClient;

    @Getter
    private UpdateHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.UPDATE;
    }

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler(HandlerConfig.builder().backoff(TEST_BACKOFF_DELAY).build());
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);

        rdsProxy = mockProxy(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsProxy.client(), atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_ModifyDbEngineVersion_NotFound() {

        when(rdsProxy.client().modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class)))
                .thenThrow(CustomDbEngineVersionNotFoundException.builder().message(MSG_NOT_FOUND).build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                null,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().description("updated").status(null).build(),
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client(), times(1)).modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class));

    }

    @Test
    public void handleRequest_ModifyThrowsRuntimeError() {

        when(rdsProxy.client().modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class)))
                .thenThrow(new RuntimeException("Internal Failure"));

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                null,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().status("invalid").build(),
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client(), times(1)).modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class));

    }

    @Test
    public void handleRequest_SimpleTagUpdate_Success() {
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                () -> DB_ENGINE_VERSION_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST_ALTER).build(),
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class));
        verify(rdsProxy.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
    }

    @Test
    public void handleRequest_ModifyStabilize() {

        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(RemoveTagsFromResourceResponse.builder().build());
        when(rdsProxy.client().modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class)))
                .thenReturn(ModifyCustomDbEngineVersionResponse.builder().build());

        final CallbackContext context = new CallbackContext();
        final AtomicBoolean fetchedOnce = new AtomicBoolean(false);

        test_handleRequest_base(
                context,
                () -> {
                    if (fetchedOnce.compareAndSet(false, true)) {
                        return DB_ENGINE_VERSION_MODIFYING;
                    }
                    return DB_ENGINE_VERSION_AVAILABLE;
                },
                () -> RESOURCE_MODEL_BUILDER().status("inactive-except-restore").tags(TAG_LIST_ALTER).build(),
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST).build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(3)).describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class));
        verify(rdsProxy.client(), times(1)).modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class));
        verify(rdsProxy.client()).addTagsToResource(any(AddTagsToResourceRequest.class));
        verify(rdsProxy.client()).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
    }

    @Test
    public void handleRequest_HardFailingTaggingOnRemoveTags() {
        when(rdsProxy.client().modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class)))
                .thenReturn(ModifyCustomDbEngineVersionResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenThrow(
                        RdsException.builder().awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.AccessDeniedException.toString()).build()).build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                () -> DB_ENGINE_VERSION_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().tags(TAG_LIST).build(),
                () -> RESOURCE_MODEL_BUILDER().status("inactive").build(),
                expectFailed(HandlerErrorCode.AccessDenied)
        );

        verify(rdsProxy.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
    }

    @Test
    public void handleRequest_HardFailingTaggingOnAddTags() {
        when(rdsProxy.client().modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class)))
                .thenReturn(ModifyCustomDbEngineVersionResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenThrow(
                        RdsException.builder().awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.AccessDeniedException.toString()).build()).build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                () -> DB_ENGINE_VERSION_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().status("inactive").tags(TAG_LIST).build(),
                expectFailed(HandlerErrorCode.AccessDenied)
        );

        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }

    @Test
    public void handleRequest_HardFailWithUnauthorizedTagsOnRemove() {
        when(rdsProxy.client().modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class)))
                .thenReturn(ModifyCustomDbEngineVersionResponse.builder().build());
        when(rdsProxy.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenThrow(
                        RdsException.builder().awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.AccessDeniedException.toString()).build()).build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousResourceTags(Translator.translateTagsToRequest(TAG_LIST))
                        .desiredResourceTags(Translator.translateTagsToRequest(TAG_LIST_EMPTY)),
                () -> DB_ENGINE_VERSION_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().status("inactive").build(),
                expectFailed(HandlerErrorCode.UnauthorizedTaggingOperation)
        );

        verify(rdsProxy.client(), times(1)).modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class));
        verify(rdsProxy.client(), times(1)).removeTagsFromResource(any(RemoveTagsFromResourceRequest.class));
    }

    @Test
    public void handleRequest_HardFailWithUnauthorizedTagsOnAdd() {
        when(rdsProxy.client().modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class)))
                .thenReturn(ModifyCustomDbEngineVersionResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenThrow(
                        RdsException.builder().awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.AccessDeniedException.toString()).build()).build());

        final CallbackContext context = new CallbackContext();

        test_handleRequest_base(
                context,
                ResourceHandlerRequest.<ResourceModel>builder()
                        .previousSystemTags(Translator.translateTagsToRequest(TAG_LIST_EMPTY))
                        .systemTags(Translator.translateTagsToRequest(TAG_LIST)),
                () -> DB_ENGINE_VERSION_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().status("inactive").build(),
                expectFailed(HandlerErrorCode.UnauthorizedTaggingOperation)
        );

        verify(rdsProxy.client(), times(1)).modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class));
        verify(rdsProxy.client(), times(1)).addTagsToResource(any(AddTagsToResourceRequest.class));
    }
}
