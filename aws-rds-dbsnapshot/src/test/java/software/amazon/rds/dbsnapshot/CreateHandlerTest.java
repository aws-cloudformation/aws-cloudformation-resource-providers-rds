package software.amazon.rds.dbsnapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.CopyDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CopyDbSnapshotResponse;
import software.amazon.awssdk.services.rds.model.CreateDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CreateDbSnapshotResponse;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DbSnapshotAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbSnapshotResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.BaseProxyClient;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractHandlerTest {

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
        handler = new CreateHandler(HandlerConfig.builder()
                .backoff(TEST_BACKOFF_DELAY)
                .build());

        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = new BaseProxyClient<>(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsProxy.client());
        verifyAccessPermissions(rdsProxy.client());
    }

    @Test
    public void handleRequest_createDBSnapshot() {
        when(rdsProxy.client().createDBSnapshot(any(CreateDbSnapshotRequest.class)))
                .thenReturn(CreateDbSnapshotResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBSnapshot.builder()
                        .status("available")
                        .build(),
                () -> ResourceModel.builder().build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBSnapshot(any(CreateDbSnapshotRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBSnapshots(any(DescribeDbSnapshotsRequest.class));
    }

    @Test
    public void handleRequest_createDBSnapshot_updateAfterCreateWithOptionGroupName() {
        when(rdsProxy.client().createDBSnapshot(any(CreateDbSnapshotRequest.class)))
                .thenReturn(CreateDbSnapshotResponse.builder().build());
        when(rdsProxy.client().modifyDBSnapshot(any(ModifyDbSnapshotRequest.class)))
                .thenReturn(ModifyDbSnapshotResponse.builder().build());

        final String optionGroupName = RandomStringUtils.randomAlphabetic(32);

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBSnapshot.builder()
                        .status("available")
                        .build(),
                () -> ResourceModel.builder()
                        .optionGroupName(optionGroupName)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBSnapshot(any(CreateDbSnapshotRequest.class));
        final ArgumentCaptor<ModifyDbSnapshotRequest> modifyCaptor = ArgumentCaptor.forClass(ModifyDbSnapshotRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBSnapshot(modifyCaptor.capture());
        verify(rdsProxy.client(), times(3)).describeDBSnapshots(any(DescribeDbSnapshotsRequest.class));

        assertThat(modifyCaptor.getValue().optionGroupName()).isEqualTo(optionGroupName);
    }

    @Test
    public void handleRequest_createDBSnapshot_updateAfterCreateWithEngineVersion() {
        when(rdsProxy.client().createDBSnapshot(any(CreateDbSnapshotRequest.class)))
                .thenReturn(CreateDbSnapshotResponse.builder().build());
        when(rdsProxy.client().modifyDBSnapshot(any(ModifyDbSnapshotRequest.class)))
                .thenReturn(ModifyDbSnapshotResponse.builder().build());

        final String engineVersion = RandomStringUtils.randomAlphabetic(32);

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBSnapshot.builder()
                        .status("available")
                        .build(),
                () -> ResourceModel.builder()
                        .engineVersion(engineVersion)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).createDBSnapshot(any(CreateDbSnapshotRequest.class));
        final ArgumentCaptor<ModifyDbSnapshotRequest> modifyCaptor = ArgumentCaptor.forClass(ModifyDbSnapshotRequest.class);
        verify(rdsProxy.client(), times(1)).modifyDBSnapshot(modifyCaptor.capture());
        verify(rdsProxy.client(), times(3)).describeDBSnapshots(any(DescribeDbSnapshotsRequest.class));

        assertThat(modifyCaptor.getValue().engineVersion()).isEqualTo(engineVersion);
    }

    @Test
    public void handleRequest_copyDBSnapshot() {
        when(rdsProxy.client().copyDBSnapshot(any(CopyDbSnapshotRequest.class)))
                .thenReturn(CopyDbSnapshotResponse.builder().build());

        final String sourceDBSnapshotIdentifier = RandomStringUtils.randomAlphabetic(10);
        final String optionGroupName = RandomStringUtils.randomAlphabetic(10);
        final List<Tag> tags = ImmutableList.of(
                Tag.builder().key("tag-key").value("tag-value").build()
        );

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBSnapshot.builder()
                        .status("available")
                        .build(),
                () -> ResourceModel.builder()
                        .sourceDBSnapshotIdentifier(sourceDBSnapshotIdentifier)
                        .optionGroupName(optionGroupName)
                        .tags(tags)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<CopyDbSnapshotRequest> copyCaptor = ArgumentCaptor.forClass(CopyDbSnapshotRequest.class);
        verify(rdsProxy.client(), times(1)).copyDBSnapshot(copyCaptor.capture());
        verify(rdsProxy.client(), times(2)).describeDBSnapshots(any(DescribeDbSnapshotsRequest.class));

        assertThat(copyCaptor.getValue().sourceDBSnapshotIdentifier()).isEqualTo(sourceDBSnapshotIdentifier);
        assertThat(copyCaptor.getValue().targetDBSnapshotIdentifier()).isNotBlank();
        assertThat(copyCaptor.getValue().optionGroupName()).isEqualTo(optionGroupName);
        assertThat(copyCaptor.getValue().tags()).containsExactly(software.amazon.awssdk.services.rds.model.Tag.builder()
                .key("tag-key")
                .value("tag-value")
                .build());
        assertThat(copyCaptor.getValue().copyTags()).isNull();
        assertThat(copyCaptor.getValue().copyOptionGroup()).isNull();
    }

    static class CreateDBSnapshotExceptionArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    Arguments.of(DbSnapshotAlreadyExistsException.builder().message(ERROR_MSG).build(), HandlerErrorCode.AlreadyExists),
                    Arguments.of(ErrorCode.DBSnapshotAlreadyExists, HandlerErrorCode.AlreadyExists),
                    Arguments.of(new RuntimeException(ERROR_MSG), HandlerErrorCode.InternalFailure),
                    Arguments.of(ErrorCode.DBSnapshotNotFound, HandlerErrorCode.NotFound)
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(CreateDBSnapshotExceptionArgumentProvider.class)
    public void handleRequest_createDBSnapshot_HandleException(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        test_handleRequest_error(
                expectCreateDBSnapshotCall(),
                new CallbackContext(),
                () -> ResourceModel.builder().build(),
                requestException,
                expectResponseCode
        );
    }
}
