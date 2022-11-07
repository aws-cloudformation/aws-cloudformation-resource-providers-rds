package software.amazon.rds.customdbengineversion;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.commons.util.StringUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.Iterables;
import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.CreateCustomDbEngineVersionRequest;
import software.amazon.awssdk.services.rds.model.CreateCustomDbEngineVersionResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsRequest;
import software.amazon.awssdk.services.rds.model.ModifyCustomDbEngineVersionRequest;
import software.amazon.awssdk.services.rds.model.ModifyCustomDbEngineVersionResponse;
import software.amazon.awssdk.services.rds.model.RdsException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    RdsClient rdsClient;

    @Getter
    private CreateHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.CREATE;
    }

    @BeforeEach
    public void setup() {
        handler = new CreateHandler(HandlerConfig.builder().backoff(TEST_BACKOFF_DELAY).build());
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = mockProxy(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsProxy.client());
    }

    @Test
    public void handleRequest_CreateSuccess() {
        when(rdsProxy.client().createCustomDBEngineVersion(any(CreateCustomDbEngineVersionRequest.class)))
                .thenReturn(CreateCustomDbEngineVersionResponse.builder().dbEngineVersionArn("arn").build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DB_ENGINE_VERSION_AVAILABLE,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createCustomDBEngineVersion(any(CreateCustomDbEngineVersionRequest.class));
    }

    @Test
    public void handleRequest_CreateSuccessWithNotAvailableStatus() {
        when(rdsProxy.client().createCustomDBEngineVersion(any(CreateCustomDbEngineVersionRequest.class)))
                .thenReturn(CreateCustomDbEngineVersionResponse.builder().dbEngineVersionArn("arn").build());
        when(rdsProxy.client().modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class)))
                .thenReturn(ModifyCustomDbEngineVersionResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DB_ENGINE_VERSION_AVAILABLE,
                () -> RESOURCE_MODEL_BUILDER().build(),
                () -> RESOURCE_MODEL_BUILDER().status("inactive").build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createCustomDBEngineVersion(any(CreateCustomDbEngineVersionRequest.class));
        verify(rdsProxy.client(), times(1)).modifyCustomDBEngineVersion(any(ModifyCustomDbEngineVersionRequest.class));
    }

    @Test
    public void handleRequest_CreateException() {
        when(rdsProxy.client().describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class)))
                .thenThrow(new RuntimeException("test exception"));

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client(), times(1)).createCustomDBEngineVersion(any(CreateCustomDbEngineVersionRequest.class));
    }

    @Test
    public void handleRequest_CreateStabilize() {
        when(rdsProxy.client().createCustomDBEngineVersion(any(CreateCustomDbEngineVersionRequest.class)))
                .thenReturn(CreateCustomDbEngineVersionResponse.builder().build());

        final AtomicBoolean fetchedOnce = new AtomicBoolean(false);

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (fetchedOnce.compareAndSet(false, true)) {
                        return DB_ENGINE_VERSION_CREATING;
                    }
                    return DB_ENGINE_VERSION_AVAILABLE;
                },
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createCustomDBEngineVersion(any(CreateCustomDbEngineVersionRequest.class));
    }

    @Test
    public void handleRequest_AccessDeniedTagging() {
        when(rdsProxy.client().createCustomDBEngineVersion(any(CreateCustomDbEngineVersionRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build())
                .thenReturn(CreateCustomDbEngineVersionResponse.builder().build());
        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(AddTagsToResourceResponse.builder().build());

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(TAG_SET.getStackTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> progress = test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags()))),
                () -> DB_ENGINE_VERSION_AVAILABLE,
                null,
                () -> RESOURCE_MODEL.toBuilder()
                        .tags(Translator.translateTagsFromSdk(TAG_SET.getResourceTags()))
                        .build(),
                expectSuccess()
        );

        Assertions.assertThat(progress.getCallbackContext().isAddTagsComplete()).isTrue();
        Assertions.assertThat(progress.getCallbackContext().getTaggingContext().isSoftFailTags()).isTrue();

        ArgumentCaptor<CreateCustomDbEngineVersionRequest> createCaptor = ArgumentCaptor.forClass(CreateCustomDbEngineVersionRequest.class);
        verify(rdsProxy.client(), times(2)).createCustomDBEngineVersion(createCaptor.capture());
        final CreateCustomDbEngineVersionRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        final CreateCustomDbEngineVersionRequest requestWithSystemTags = createCaptor.getAllValues().get(1);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(TAG_SET), software.amazon.awssdk.services.rds.model.Tag.class));
        Assertions.assertThat(requestWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class));

        verify(rdsProxy.client(), times(2)).describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class));

        ArgumentCaptor<AddTagsToResourceRequest> addTagsCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client(), times(1)).addTagsToResource(addTagsCaptor.capture());
        Assertions.assertThat(addTagsCaptor.getValue().tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(extraTags), software.amazon.awssdk.services.rds.model.Tag.class));
    }


    @Test
    public void handleRequest_HardFailTagging() {
        when(rdsProxy.client().createCustomDBEngineVersion(any(CreateCustomDbEngineVersionRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build())
                .thenReturn(CreateCustomDbEngineVersionResponse.builder().build());

        when(rdsProxy.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenThrow(
                        RdsException.builder()
                                .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(ErrorCode.AccessDeniedException.toString())
                                        .build()
                                ).build());

        final Tagging.TagSet extraTags = Tagging.TagSet.builder()
                .stackTags(TAG_SET.getStackTags())
                .resourceTags(TAG_SET.getResourceTags())
                .build();

        test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder()
                        .systemTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getSystemTags())))
                        .desiredResourceTags(Translator.translateTagsToRequest(Translator.translateTagsFromSdk(TAG_SET.getStackTags()))),
                () -> DB_ENGINE_VERSION_AVAILABLE,
                null,
                () -> RESOURCE_MODEL.toBuilder()
                        .tags(Translator.translateTagsFromSdk(TAG_SET.getResourceTags()))
                        .build(),
                expectFailed(HandlerErrorCode.AccessDenied)
        );

        ArgumentCaptor<CreateCustomDbEngineVersionRequest> createCaptor = ArgumentCaptor.forClass(CreateCustomDbEngineVersionRequest.class);
        verify(rdsProxy.client(), times(2)).createCustomDBEngineVersion(createCaptor.capture());
        final CreateCustomDbEngineVersionRequest requestWithAllTags = createCaptor.getAllValues().get(0);
        final CreateCustomDbEngineVersionRequest requestWithSystemTags = createCaptor.getAllValues().get(1);
        Assertions.assertThat(requestWithAllTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(TAG_SET), software.amazon.awssdk.services.rds.model.Tag.class));
        Assertions.assertThat(requestWithSystemTags.tags()).containsExactlyInAnyOrder(
                Iterables.toArray(TAG_SET.getSystemTags(), software.amazon.awssdk.services.rds.model.Tag.class));

        verify(rdsProxy.client(), times(1)).describeDBEngineVersions(any(DescribeDbEngineVersionsRequest.class));

        ArgumentCaptor<AddTagsToResourceRequest> addTagsCaptor = ArgumentCaptor.forClass(AddTagsToResourceRequest.class);
        verify(rdsProxy.client(), times(1)).addTagsToResource(addTagsCaptor.capture());
        Assertions.assertThat(addTagsCaptor.getValue().tags()).containsExactlyInAnyOrder(
                Iterables.toArray(Tagging.translateTagsToSdk(extraTags), software.amazon.awssdk.services.rds.model.Tag.class));
    }
}
