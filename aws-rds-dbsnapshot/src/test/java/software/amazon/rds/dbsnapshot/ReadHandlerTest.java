package software.amazon.rds.dbsnapshot;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.time.Duration;
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

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DbSnapshotNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.BaseProxyClient;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractHandlerTest {

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
    private ReadHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.READ;
    }

    @BeforeEach
    public void setup() {
        handler = new ReadHandler(HandlerConfig.builder()
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
    public void handleRequest_describeDBSnapshot() {
        final String dbSnapshotIdentifier = RandomStringUtils.randomAlphabetic(32);

        test_handleRequest_base(
                new CallbackContext(),
                () -> DBSnapshot.builder()
                        .dbSnapshotIdentifier(dbSnapshotIdentifier)
                        .build(),
                () -> ResourceModel.builder()
                        .dBSnapshotIdentifier(dbSnapshotIdentifier)
                        .build(),
                expectSuccess()
        );

        final ArgumentCaptor<DescribeDbSnapshotsRequest> describeCaptor = ArgumentCaptor.forClass(DescribeDbSnapshotsRequest.class);
        verify(rdsProxy.client(), times(1)).describeDBSnapshots(describeCaptor.capture());

        assertThat(describeCaptor.getValue().dbSnapshotIdentifier()).isEqualTo(dbSnapshotIdentifier);
    }

    static class ReadDBSnapshotExceptionArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    Arguments.of(new RuntimeException(ERROR_MSG), HandlerErrorCode.InternalFailure),
                    Arguments.of(ErrorCode.DBSnapshotNotFound, HandlerErrorCode.NotFound),
                    Arguments.of(DbSnapshotNotFoundException.builder().message(ERROR_MSG).build(), HandlerErrorCode.NotFound)
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(ReadDBSnapshotExceptionArgumentProvider.class)
    public void handleRequest_createDBSnapshot_HandleException(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        test_handleRequest_error(
                expectDescribeDBSnapshotsCall(),
                new CallbackContext(),
                () -> ResourceModel.builder().build(),
                requestException,
                expectResponseCode
        );
    }
}
