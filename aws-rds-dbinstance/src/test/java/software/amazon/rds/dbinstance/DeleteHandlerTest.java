package software.amazon.rds.dbinstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.InvalidDbInstanceStateException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.common.test.TestUtils;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    @Getter
    private ProxyClient<Ec2Client> ec2Proxy;

    @Mock
    private RdsClient rdsClient;

    @Mock
    private Ec2Client ec2Client;

    @Getter
    private DeleteHandler handler;

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler(
                HandlerConfig.builder()
                        .probingEnabled(false)
                        .backoff(TEST_BACKOFF_DELAY)
                        .build()
        );
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        ec2Client = mock(Ec2Client.class);
        rdsProxy = mockProxy(proxy, rdsClient);
        ec2Proxy = mockProxy(proxy, ec2Client);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyNoMoreInteractions(ec2Client);
    }

    @Test
    public void handleRequest_Success() {
        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    throw DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build();
                },
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        assertThat(response.getMessage()).isNull();

        verify(rdsProxy.client(), times(1)).deleteDBInstance(any(DeleteDbInstanceRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_DbInstanceNotFound() {
        final DbInstanceNotFoundException exception = DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenThrow(exception);

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client(), times(1)).deleteDBInstance(any(DeleteDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_DBInstanceIsBeingDeleted() {
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenThrow(
                InvalidDbInstanceStateException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.InvalidDBInstanceState.toString())
                                .errorMessage("Instance foo-bar is already being deleted.")
                                .build()
                        ).build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    throw DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build();
                },
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).deleteDBInstance(any(DeleteDbInstanceRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }


    @Test
    public void handleRequest_IsDeleting_Stabilize() {
        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        AtomicBoolean fetchedOnce = new AtomicBoolean(false);
        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    if (fetchedOnce.compareAndSet(false, true)) {
                        return DB_INSTANCE_DELETING;
                    }
                    throw DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build();
                },
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        assertThat(response.getMessage()).isNull();

        verify(rdsProxy.client(), times(1)).deleteDBInstance(any(DeleteDbInstanceRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_SkipFinalSnapshot() {
        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().snapshotRequested(false),
                () -> {
                    throw DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build();
                },
                null,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbInstanceRequest> argument = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(argument.capture());
        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(argument.getValue().skipFinalSnapshot()).isTrue();
        assertThat(argument.getValue().finalDBSnapshotIdentifier()).isNull();
    }

    @Test
    public void handleRequest_RequestFinalSnapshotIfNotExplicitlyRequested() {
        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().snapshotRequested(null),
                () -> {
                    throw DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build();
                },
                null,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbInstanceRequest> argument = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(argument.capture());
        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(argument.getValue().skipFinalSnapshot()).isFalse();
        assertThat(argument.getValue().finalDBSnapshotIdentifier()).isNotNull();
    }

    @Test
    public void handleRequest_NoFinalSnapshotForClusterInstance() {
        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        final String dbClusterIdentifier = TestUtils.randomString(64, TestUtils.ALPHA);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().snapshotRequested(true),
                () -> {
                    throw DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build();
                },
                null,
                () -> RESOURCE_MODEL_BLDR().dBClusterIdentifier(dbClusterIdentifier).build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbInstanceRequest> argument = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(argument.capture());
        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(argument.getValue().skipFinalSnapshot()).isTrue();
        assertThat(argument.getValue().finalDBSnapshotIdentifier()).isNull();
    }

    @Test
    public void handleRequest_NoFinalSnapshotForReadReplica() {
        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        final String sourceDBInstanceIdentifier = TestUtils.randomString(64, TestUtils.ALPHA);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().snapshotRequested(true),
                () -> {
                    throw DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build();
                },
                null,
                () -> RESOURCE_MODEL_BLDR().sourceDBInstanceIdentifier(sourceDBInstanceIdentifier).build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbInstanceRequest> argument = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(argument.capture());
        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(argument.getValue().skipFinalSnapshot()).isTrue();
        assertThat(argument.getValue().finalDBSnapshotIdentifier()).isNull();
    }

    @Test
    public void handleRequest_DeleteAutomatedBackups() {
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class)))
                .thenReturn(DeleteDbInstanceResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    throw DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build();
                },
                null,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .deleteAutomatedBackups(true)
                        .build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbInstanceRequest> argument = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(argument.capture());
        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(argument.getValue().deleteAutomatedBackups()).isTrue();
    }

    static class DeleteDBInstanceExceptionArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    // Put error codes below
                    Arguments.of(ErrorCode.DBSnapshotAlreadyExists, HandlerErrorCode.InvalidRequest),
                    Arguments.of(ErrorCode.InvalidDBInstanceState, HandlerErrorCode.ResourceConflict),
                    Arguments.of(ErrorCode.InvalidParameterValue, HandlerErrorCode.InvalidRequest),
                    // Put exception classes below
                    Arguments.of(new RuntimeException(MSG_GENERIC_ERR), HandlerErrorCode.InternalFailure)
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(DeleteDBInstanceExceptionArgumentProvider.class)
    public void handleRequest_DeleteDBInstance_HandleException(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        test_handleRequest_error(
                expectDeleteDBInstanceCall(),
                new CallbackContext(),
                () -> RESOURCE_MODEL_BLDR().build(),
                requestException,
                expectResponseCode
        );
    }
}
