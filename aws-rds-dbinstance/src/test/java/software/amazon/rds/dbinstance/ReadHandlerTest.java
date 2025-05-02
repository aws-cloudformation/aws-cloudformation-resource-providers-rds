package software.amazon.rds.dbinstance;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;
import java.util.function.Supplier;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.cloudwatchlogs.model.AccessDeniedException;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstanceAutomatedBackup;
import software.amazon.awssdk.services.rds.model.DBInstanceAutomatedBackupsReplication;
import software.amazon.awssdk.services.rds.model.DbInstanceAutomatedBackupNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbInstanceAutomatedBackupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstanceAutomatedBackupsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractHandlerTest {

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
    private ReadHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.READ;
    }

    @BeforeEach
    public void setup() {
        handler = new ReadHandler();
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
        verifyAccessPermissions(rdsClient);
        verifyAccessPermissions(ec2Client);
    }

    @Test
    public void handleRequest_ReadSuccess() {
        test_handleRequest_base(
                new CallbackContext(),
                () -> DB_INSTANCE_ACTIVE,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_ValidAutomaticBackupReplicationArn() {
        proxy = Mockito.spy(proxy);
        final RdsClient crossRegionRdsClient = mock(RdsClient.class);
        final ProxyClient<RdsClient> crossRegionRdsProxy = mockProxy(proxy, crossRegionRdsClient);
        doReturn(crossRegionRdsProxy).when(proxy).newProxy(ArgumentMatchers.<Supplier<RdsClient>>any());

        final CallbackContext context = new CallbackContext();
        final String automaticBackupReplicationArn = getAutomaticBackupArn(AUTOMATIC_BACKUP_REPLICATION_REGION);

        when(crossRegionRdsProxy.client().describeDBInstanceAutomatedBackups(any(DescribeDbInstanceAutomatedBackupsRequest.class)))
            .thenReturn(DescribeDbInstanceAutomatedBackupsResponse.builder()
                .dbInstanceAutomatedBackups(Collections.singletonList(DBInstanceAutomatedBackup.builder()
                    .dbInstanceAutomatedBackupsArn(
                        getAutomaticBackupArn(AUTOMATIC_BACKUP_REPLICATION_REGION))
                    .backupRetentionPeriod(AUTOMATIC_BACKUP_REPLICATION_RETENTION_PERIOD).build()))
                .build());

        test_handleRequest_base(
            context,
            () -> DB_INSTANCE_ACTIVE.toBuilder()
                .dbInstanceAutomatedBackupsReplications(Collections.singletonList(DBInstanceAutomatedBackupsReplication.builder()
                    .dbInstanceAutomatedBackupsArn(automaticBackupReplicationArn).build()))
                .build(),
            () -> RESOURCE_MODEL_BLDR().build(),
            expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(crossRegionRdsProxy.client(), times(1)).describeDBInstanceAutomatedBackups(any(DescribeDbInstanceAutomatedBackupsRequest.class));
        Assertions.assertThat(context.isAutomaticBackupReplicationStarted()).isTrue();
    }

    @Test
    public void handleRequest_AutomaticBackupReplicationNotFound() {
        // This tests the edge case  where the describe db instance automated backups request returns not found.
        // In this scenario, the request will fail and simply progress to 'success' without replication parameters.
        proxy = Mockito.spy(proxy);
        final RdsClient crossRegionRdsClient = mock(RdsClient.class);
        final ProxyClient<RdsClient> crossRegionRdsProxy = mockProxy(proxy, crossRegionRdsClient);
        doReturn(crossRegionRdsProxy).when(proxy).newProxy(ArgumentMatchers.<Supplier<RdsClient>>any());

        final CallbackContext context = new CallbackContext();
        final String automaticBackupReplicationArn = getAutomaticBackupArn(AUTOMATIC_BACKUP_REPLICATION_REGION);

        when(crossRegionRdsProxy.client().describeDBInstanceAutomatedBackups(any(DescribeDbInstanceAutomatedBackupsRequest.class)))
            .thenThrow(DbInstanceAutomatedBackupNotFoundException.class);

        test_handleRequest_base(
            context,
            () -> DB_INSTANCE_ACTIVE.toBuilder()
                .dbInstanceAutomatedBackupsReplications(Collections.singletonList(DBInstanceAutomatedBackupsReplication.builder()
                    .dbInstanceAutomatedBackupsArn(automaticBackupReplicationArn).build()))
                .build(),
            () -> RESOURCE_MODEL_BLDR().build(),
            expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(crossRegionRdsProxy.client(), times(1)).describeDBInstanceAutomatedBackups(any(DescribeDbInstanceAutomatedBackupsRequest.class));
        Assertions.assertThat(context.isAutomaticBackupReplicationStarted()).isFalse();
    }

    @Test
    public void handleRequest_AutomaticBackupReplicationAccessDenied() {
        // This tests the edge case  where the DB instance has replications, but customer does not have permission to run DescribeDBInstanceAutomatedBackups
        // In this scenario, the request will fail and simply progress to 'success' without replication parameters.
        proxy = Mockito.spy(proxy);
        final RdsClient crossRegionRdsClient = mock(RdsClient.class);
        final ProxyClient<RdsClient> crossRegionRdsProxy = mockProxy(proxy, crossRegionRdsClient);
        doReturn(crossRegionRdsProxy).when(proxy).newProxy(ArgumentMatchers.<Supplier<RdsClient>>any());

        final CallbackContext context = new CallbackContext();
        final String automaticBackupReplicationArn = getAutomaticBackupArn(AUTOMATIC_BACKUP_REPLICATION_REGION);

        when(crossRegionRdsProxy.client().describeDBInstanceAutomatedBackups(any(DescribeDbInstanceAutomatedBackupsRequest.class)))
            .thenThrow(AwsServiceException.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("AccessDenied").build())
                .build());

        test_handleRequest_base(
            context,
            () -> DB_INSTANCE_ACTIVE.toBuilder()
                .dbInstanceAutomatedBackupsReplications(Collections.singletonList(DBInstanceAutomatedBackupsReplication.builder()
                    .dbInstanceAutomatedBackupsArn(automaticBackupReplicationArn).build()))
                .build(),
            () -> RESOURCE_MODEL_BLDR().build(),
            expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
        verify(crossRegionRdsProxy.client(), times(1)).describeDBInstanceAutomatedBackups(any(DescribeDbInstanceAutomatedBackupsRequest.class));
        Assertions.assertThat(context.isAutomaticBackupReplicationStarted()).isFalse();
    }

    @ParameterizedTest
    @ArgumentsSource(ThrottleExceptionArgumentsProvider.class)
    public void handleRequest_DescribeDBInstance_HandleThrottleException(
        final Object requestException
    ) {
        test_handleRequest_error(
            expectDescribeDBInstancesCall(),
            new CallbackContext(),
            () -> RESOURCE_MODEL_BLDR().build(),
            requestException,
            HandlerErrorCode.Throttling
        );
    }
}
