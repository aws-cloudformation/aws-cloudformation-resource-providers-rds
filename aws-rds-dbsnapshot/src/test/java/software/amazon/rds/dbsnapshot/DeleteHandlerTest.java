package software.amazon.rds.dbsnapshot;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbSnapshotResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.BaseProxyClient;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractHandlerTest {

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
    private DeleteHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.DELETE;
    }

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler(HandlerConfig.builder()
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
    public void handleRequest_DeleteDBSnapshot() {
        when(rdsProxy.client().deleteDBSnapshot(any(DeleteDbSnapshotRequest.class)))
                .thenReturn(DeleteDbSnapshotResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    throw DbSnapshotNotFoundException.builder().message(ERROR_MSG).build();
                },
                () -> ResourceModel.builder().build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).deleteDBSnapshot(any(DeleteDbSnapshotRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBSnapshots(any(DescribeDbSnapshotsRequest.class));
    }

    static class DeleteDBSnapshotExceptionArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    Arguments.of(DbSnapshotNotFoundException.builder().message(ERROR_MSG).build(), HandlerErrorCode.NotFound),
                    Arguments.of(ErrorCode.DBSnapshotNotFound, HandlerErrorCode.NotFound),
                    Arguments.of(new RuntimeException(ERROR_MSG), HandlerErrorCode.InternalFailure)
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(DeleteDBSnapshotExceptionArgumentProvider.class)
    public void handleRequest_deleteDBSnapshot_HandleException(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        test_handleRequest_error(
                expectDeleteDBSnapshotCall(),
                new CallbackContext(),
                () -> ResourceModel.builder().build(),
                requestException,
                expectResponseCode
        );
    }
}
