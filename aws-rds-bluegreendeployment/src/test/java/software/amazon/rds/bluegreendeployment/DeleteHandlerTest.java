package software.amazon.rds.bluegreendeployment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.arn.Arn;
import com.amazonaws.arn.ArnResource;
import com.amazonaws.regions.Regions;
import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.BlueGreenDeployment;
import software.amazon.awssdk.services.rds.model.BlueGreenDeploymentNotFoundException;
import software.amazon.awssdk.services.rds.model.DbInstanceNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteBlueGreenDeploymentRequest;
import software.amazon.awssdk.services.rds.model.DeleteBlueGreenDeploymentResponse;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbInstanceResponse;
import software.amazon.awssdk.services.rds.model.DescribeBlueGreenDeploymentsRequest;
import software.amazon.awssdk.services.rds.model.DescribeBlueGreenDeploymentsResponse;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;
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
        handler = new DeleteHandler(
                HandlerConfig.builder()
                        .backoff(Constant.of()
                                .delay(Duration.ofMillis(1))
                                .timeout(Duration.ofSeconds(10))
                                .build())
                        .build()
        );
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_deleteBlueGreenDeployment() {
        when(rdsProxy.client().deleteBlueGreenDeployment(any(DeleteBlueGreenDeploymentRequest.class)))
                .thenReturn(DeleteBlueGreenDeploymentResponse.builder().build());

        when(rdsProxy.client().describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class)))
                .thenReturn(DescribeBlueGreenDeploymentsResponse.builder()
                        .blueGreenDeployments(BlueGreenDeployment.builder().build())
                        .build())
                .thenThrow(BlueGreenDeploymentNotFoundException.builder()
                        .message("Not found")
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> ResourceModel.builder()
                        .blueGreenDeploymentIdentifier("blue-green-deployment-identifier")
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).deleteBlueGreenDeployment(any(DeleteBlueGreenDeploymentRequest.class));
        verify(rdsProxy.client(), times(2)).describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class));
    }

    @Test
    public void handleRequest_deleteBlueGreenDeploymentWithDeleteSourceCompletedStatusShouldDeleteSource() {
        when(rdsProxy.client().deleteBlueGreenDeployment(any(DeleteBlueGreenDeploymentRequest.class)))
                .thenReturn(DeleteBlueGreenDeploymentResponse.builder().build());

        when(rdsProxy.client().describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class)))
                .thenReturn(DescribeBlueGreenDeploymentsResponse.builder()
                        .blueGreenDeployments(BlueGreenDeployment.builder()
                                .status(BlueGreenDeploymentStatus.SwitchoverCompleted.toString())
                                .build())
                        .build())
                .thenThrow(BlueGreenDeploymentNotFoundException.builder()
                        .message("Not found")
                        .build());

        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class)))
                .thenReturn(DeleteDbInstanceResponse.builder().build());

        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenThrow(DbInstanceNotFoundException.builder()
                        .message("Not found")
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> ResourceModel.builder()
                        .blueGreenDeploymentIdentifier("blue-green-deployment-identifier")
                        .source("source-db-instance")
                        .deleteSource(true)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).deleteBlueGreenDeployment(any(DeleteBlueGreenDeploymentRequest.class));
        verify(rdsProxy.client(), times(1)).deleteDBInstance(any(DeleteDbInstanceRequest.class));
        verify(rdsProxy.client(), times(2)).describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }

    @Test
    public void handleRequest_deleteBlueGreenDeploymentWithInstanceArnAndDeleteSourceCompletedStatusShouldDeleteSource() {
        final String dbInstanceIdentifier = RandomStringUtils.randomAlphabetic(32);
        final Arn dbInstanceArn = Arn.builder()
                .withPartition("aws")
                .withService("rds")
                .withAccountId("1234567890")
                .withRegion(Regions.US_EAST_1.getName())
                .withResource(ArnResource.builder().withResourceType("db").withResource(dbInstanceIdentifier).build().toString())
                .build();

        when(rdsProxy.client().deleteBlueGreenDeployment(any(DeleteBlueGreenDeploymentRequest.class)))
                .thenReturn(DeleteBlueGreenDeploymentResponse.builder().build());

        when(rdsProxy.client().describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class)))
                .thenReturn(DescribeBlueGreenDeploymentsResponse.builder()
                        .blueGreenDeployments(BlueGreenDeployment.builder()
                                .source(dbInstanceArn.toString())
                                .status(BlueGreenDeploymentStatus.SwitchoverCompleted.toString())
                                .build())
                        .build())
                .thenThrow(BlueGreenDeploymentNotFoundException.builder()
                        .message("Not found")
                        .build());

        when(rdsProxy.client().deleteDBInstance(any(DeleteDbInstanceRequest.class)))
                .thenReturn(DeleteDbInstanceResponse.builder().build());

        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenThrow(DbInstanceNotFoundException.builder()
                        .message("Not found")
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> ResourceModel.builder()
                        .blueGreenDeploymentIdentifier("blue-green-deployment-identifier")
                        .source(dbInstanceArn.toString())
                        .deleteSource(true)
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).deleteBlueGreenDeployment(any(DeleteBlueGreenDeploymentRequest.class));
        final ArgumentCaptor<DeleteDbInstanceRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteDbInstanceRequest.class);
        verify(rdsProxy.client(), times(1)).deleteDBInstance(deleteCaptor.capture());
        verify(rdsProxy.client(), times(2)).describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class));
        verify(rdsProxy.client(), times(1)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(deleteCaptor.getValue().dbInstanceIdentifier()).isEqualTo(dbInstanceIdentifier);
    }

    @Test
    public void handleRequest_deleteBlueGreenDeploymentWithDeleteSourceOtherStatusShouldNotDeleteSource() {
        when(rdsProxy.client().deleteBlueGreenDeployment(any(DeleteBlueGreenDeploymentRequest.class)))
                .thenReturn(DeleteBlueGreenDeploymentResponse.builder().build());

        when(rdsProxy.client().describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class)))
                .thenReturn(DescribeBlueGreenDeploymentsResponse.builder()
                        .blueGreenDeployments(BlueGreenDeployment.builder()
                                .status(BlueGreenDeploymentStatus.Available.toString())
                                .build())
                        .build())
                .thenThrow(BlueGreenDeploymentNotFoundException.builder()
                        .message("Not found")
                        .build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> ResourceModel.builder()
                        .blueGreenDeploymentIdentifier("blue-green-deployment-identifier")
                        .source("source-db-instance")
                        .deleteSource(true)
                        .build(),
                expectSuccess()
        );

        // There should be no interactions with the source DBInstance as the BlueGreenDeployment status is not "switchover-completed"
        verify(rdsProxy.client(), times(1)).deleteBlueGreenDeployment(any(DeleteBlueGreenDeploymentRequest.class));
        verify(rdsProxy.client(), times(2)).describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class));
    }

    @Test
    public void handleRequest_deleteBlueGreenDeploymentDeleteTargetOtherStatusShouldDeleteTarget() {
        when(rdsProxy.client().describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class)))
                .thenReturn(DescribeBlueGreenDeploymentsResponse.builder()
                        .blueGreenDeployments(BlueGreenDeployment.builder()
                                .status(BlueGreenDeploymentStatus.Available.toString())
                                .build())
                        .build())
                .thenThrow(BlueGreenDeploymentNotFoundException.builder()
                        .message("Not found")
                        .build());

        when(rdsProxy.client().deleteBlueGreenDeployment(any(DeleteBlueGreenDeploymentRequest.class)))
                .thenReturn(DeleteBlueGreenDeploymentResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> ResourceModel.builder()
                        .blueGreenDeploymentIdentifier("blue-green-deployment-identifier")
                        .deleteTarget(true)
                        .build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteBlueGreenDeploymentRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteBlueGreenDeploymentRequest.class);
        verify(rdsProxy.client(), times(1)).deleteBlueGreenDeployment(deleteCaptor.capture());
        verify(rdsProxy.client(), times(2)).describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class));

        assertThat(deleteCaptor.getValue().deleteTarget()).isTrue();
    }

    @Test
    public void handleRequest_deleteBlueGreenDeploymentDeleteTargetCompletedStatusShouldNotDeleteTarget() {
        when(rdsProxy.client().describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class)))
                .thenReturn(DescribeBlueGreenDeploymentsResponse.builder()
                        .blueGreenDeployments(BlueGreenDeployment.builder()
                                .status(BlueGreenDeploymentStatus.SwitchoverCompleted.toString())
                                .build())
                        .build())
                .thenThrow(BlueGreenDeploymentNotFoundException.builder()
                        .message("Not found")
                        .build());

        when(rdsProxy.client().deleteBlueGreenDeployment(any(DeleteBlueGreenDeploymentRequest.class)))
                .thenReturn(DeleteBlueGreenDeploymentResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> ResourceModel.builder()
                        .blueGreenDeploymentIdentifier("blue-green-deployment-identifier")
                        .deleteTarget(true)
                        .build(),
                expectSuccess()
        );

        final ArgumentCaptor<DeleteBlueGreenDeploymentRequest> deleteCaptor = ArgumentCaptor.forClass(DeleteBlueGreenDeploymentRequest.class);
        verify(rdsProxy.client(), times(1)).deleteBlueGreenDeployment(deleteCaptor.capture());
        verify(rdsProxy.client(), times(2)).describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class));

        assertThat(deleteCaptor.getValue().deleteTarget()).isFalse();
    }
}
