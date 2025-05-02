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
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.awssdk.services.rds.model.InvalidDbInstanceStateException;
import software.amazon.awssdk.services.rds.model.OptionGroupMembership;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.dbinstance.status.OptionGroupStatus;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.TestUtils;

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

    private boolean expectServiceInvocation;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.DELETE;
    }

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
        expectServiceInvocation = true;
    }

    @AfterEach
    public void tear_down() {
        if (expectServiceInvocation) {
            verify(rdsClient, atLeastOnce()).serviceName();
        }
        verifyNoMoreInteractions(rdsClient);
        verifyNoMoreInteractions(ec2Client);
        verifyAccessPermissions(rdsClient);
        verifyAccessPermissions(ec2Client);
    }

    @Test
    public void handleRequest_Success() {
        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_ACTIVE).build())
                .thenThrow(DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        assertThat(response.getMessage()).isNull();

        verify(rdsProxy.client(), times(1)).deleteDBInstance(any(DeleteDbInstanceRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_DbInstanceNotFoundDuringDescribe() {
        expectServiceInvocation = false;

        test_handleRequest_base(
                new CallbackContext(),
                () -> {
                    throw DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build();
                },
                () -> RESOURCE_MODEL_BLDR().build(),
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_DbInstanceNotFoundDuringDelete() {
        final DbInstanceNotFoundException exception = DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenThrow(exception);

        test_handleRequest_base(
                new CallbackContext(),
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(rdsProxy.client(), times(1)).deleteDBInstance(any(DeleteDbInstanceRequest.class));
    }

    @Test
    public void handleRequest_DBInstanceIsBeingDeleted() {
        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_ACTIVE).build())
                .thenThrow(DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenThrow(
                InvalidDbInstanceStateException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.InvalidDBInstanceState.toString())
                                .errorMessage("Instance foo-bar is already being deleted.")
                                .build()
                        ).build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).deleteDBInstance(any(DeleteDbInstanceRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }


    @Test
    public void handleRequest_IsDeleting_Stabilize() {
        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_ACTIVE).build())
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_DELETING).build())
                .thenThrow(DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        assertThat(response.getMessage()).isNull();

        verify(rdsProxy.client(), times(1)).deleteDBInstance(any(DeleteDbInstanceRequest.class));
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_SkipFinalSnapshot() {
        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_ACTIVE).build())
                .thenThrow(DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().snapshotRequested(false),
                null,
                null,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbInstanceRequest> argument = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(argument.capture());
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(argument.getValue().skipFinalSnapshot()).isTrue();
        assertThat(argument.getValue().finalDBSnapshotIdentifier()).isNull();
    }

    @Test
    public void handleRequest_RequestFinalSnapshotIfNotExplicitlyRequested() {
        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_ACTIVE).build())
                .thenThrow(DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().snapshotRequested(null),
                null,
                null,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbInstanceRequest> argument = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(argument.capture());
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(argument.getValue().skipFinalSnapshot()).isFalse();
        assertThat(argument.getValue().finalDBSnapshotIdentifier()).isNotNull();
    }

    @Test
    public void handleRequest_NoFinalSnapshotForClusterInstance() {
        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_AURORA_ACTIVE).build())
                .thenThrow(DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        final String dbClusterIdentifier = TestUtils.randomString(64, TestUtils.ALPHA);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().snapshotRequested(true),
                null,
                null,
                () -> RESOURCE_MODEL_BLDR().dBClusterIdentifier(dbClusterIdentifier).build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbInstanceRequest> argument = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(argument.capture());
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(argument.getValue().skipFinalSnapshot()).isTrue();
        assertThat(argument.getValue().finalDBSnapshotIdentifier()).isNull();
    }

    @Test
    public void handleRequest_NoFinalSnapshotForReadReplica() {
        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_READ_REPLICA_ACTIVE).build())
                .thenThrow(DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        final DeleteDbInstanceResponse deleteDbInstanceResponse = DeleteDbInstanceResponse.builder().build();
        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class))).thenReturn(deleteDbInstanceResponse);

        final String sourceDBInstanceIdentifier = TestUtils.randomString(64, TestUtils.ALPHA);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                ResourceHandlerRequest.<ResourceModel>builder().snapshotRequested(true),
                null,
                null,
                () -> RESOURCE_MODEL_BLDR().sourceDBInstanceIdentifier(sourceDBInstanceIdentifier).build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbInstanceRequest> argument = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(argument.capture());
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(argument.getValue().skipFinalSnapshot()).isTrue();
        assertThat(argument.getValue().finalDBSnapshotIdentifier()).isNull();
    }

    @Test
    public void handleRequest_DeleteAutomatedBackups() {
        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_ACTIVE).build())
                .thenThrow(DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class)))
                .thenReturn(DeleteDbInstanceResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                null,
                () -> RESOURCE_MODEL_BAREBONE_BLDR()
                        .deleteAutomatedBackups(true)
                        .build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbInstanceRequest> argument = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(argument.capture());
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(argument.getValue().deleteAutomatedBackups()).isTrue();
    }

    @Test
    public void handleRequest_DeleteDBInstance_DbInstanceInTerminalState() {
        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_FAILED).build())
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_FAILED).build())
                .thenThrow(DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_BAREBONE_BLDR().build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbInstanceRequest> argument = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(argument.capture());
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_DeleteDBInstance_OptionGroupInTerminalState() {
        var optionGroupFailedInstance = DB_INSTANCE_DELETING.toBuilder()
                .optionGroupMemberships(OptionGroupMembership.builder()
                        .status(OptionGroupStatus.Failed.toString())
                        .optionGroupName(OPTION_GROUP_NAME_MYSQL_DEFAULT)
                        .build()).build();

        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(optionGroupFailedInstance).build())
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(optionGroupFailedInstance).build())
                .thenThrow(DbInstanceNotFoundException.builder().message(MSG_NOT_FOUND_ERR).build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_BAREBONE_BLDR().build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteDbInstanceRequest> argument = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(argument.capture());
        verify(rdsProxy.client(), times(3)).describeDBInstances(any(DescribeDbInstancesRequest.class));
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
    public void handleRequest_DeleteDBInstance_HandleExceptionInDelete(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_ACTIVE).build());

        test_handleRequest_error(
                expectDeleteDBInstanceCall(),
                new CallbackContext(),
                () -> RESOURCE_MODEL_BLDR().build(),
                requestException,
                expectResponseCode
        );
    }

    static class DescribeDBInstanceExceptionArgumentProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
            return Stream.of(
                    // Put error codes below
                    Arguments.of(ErrorCode.InvalidParameterValue, HandlerErrorCode.InvalidRequest),
                    // Put exception classes below
                    Arguments.of(new RuntimeException(MSG_GENERIC_ERR), HandlerErrorCode.InternalFailure)
            );
        }
    }

    @ParameterizedTest
    @ArgumentsSource(DescribeDBInstanceExceptionArgumentProvider.class)
    public void handleRequest_DeleteDBInstance_HandleExceptionInDescribe(
            final Object requestException,
            final HandlerErrorCode expectResponseCode
    ) {
        expectServiceInvocation = false;
        test_handleRequest_error(
                expectDescribeDBInstancesCall(),
                new CallbackContext(),
                () -> RESOURCE_MODEL_BLDR().build(),
                requestException,
                expectResponseCode
        );
    }

    @ParameterizedTest
    @ArgumentsSource(ThrottleExceptionArgumentsProvider.class)
    public void handleRequest_DeleteDBInstance_HandleThrottleExceptionInDelete(
        final Object requestException
    ) {
        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
            .thenReturn(DescribeDbInstancesResponse.builder().dbInstances(DB_INSTANCE_ACTIVE).build());

        test_handleRequest_throttle(
            expectDeleteDBInstanceCall(),
            new CallbackContext(),
            () -> RESOURCE_MODEL_BLDR().build(),
            requestException,
            CALLBACK_DELAY
        );
    }

    @ParameterizedTest
    @ArgumentsSource(ThrottleExceptionArgumentsProvider.class)
    public void handleRequest_DeleteDBInstance_HandleThrottleExceptionInDescribe(
        final Object requestException
    ) {
        expectServiceInvocation = false;
        test_handleRequest_throttle(
            expectDescribeDBInstancesCall(),
            new CallbackContext(),
            () -> RESOURCE_MODEL_BLDR().build(),
            requestException,
            CALLBACK_DELAY
        );
    }
}
