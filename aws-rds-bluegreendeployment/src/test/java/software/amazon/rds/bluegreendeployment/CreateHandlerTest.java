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

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.BlueGreenDeployment;
import software.amazon.awssdk.services.rds.model.CreateBlueGreenDeploymentRequest;
import software.amazon.awssdk.services.rds.model.CreateBlueGreenDeploymentResponse;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeBlueGreenDeploymentsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;
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
        handler = new CreateHandler(
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
    public void handleRequest_createBlueGreenDeploymentShouldSetRandomBlueGreenDeploymentName() {
        final String blueGreenDeploymentIdentifier = RandomStringUtils.randomAlphabetic(32);

        when(rdsProxy.client().createBlueGreenDeployment(any(CreateBlueGreenDeploymentRequest.class)))
                .thenReturn(CreateBlueGreenDeploymentResponse.builder()
                        .blueGreenDeployment(BlueGreenDeployment.builder()
                                .blueGreenDeploymentIdentifier(blueGreenDeploymentIdentifier)
                                .build())
                        .build());

        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder()
                        .dbInstances(DBInstance.builder()
                                .dbInstanceStatus("available")
                                .build())
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                () -> BlueGreenDeployment.builder()
                        .status("available")
                        .blueGreenDeploymentIdentifier(blueGreenDeploymentIdentifier)
                        .target("target-db-instance-identifier")
                        .source("source-db-instance-identifier")
                        .build(),
                () -> ResourceModel.builder().build(),
                expectSuccess()
        );

        ArgumentCaptor<CreateBlueGreenDeploymentRequest> createCaptor = ArgumentCaptor.forClass(CreateBlueGreenDeploymentRequest.class);
        verify(rdsProxy.client(), times(1)).createBlueGreenDeployment(createCaptor.capture());
        verify(rdsProxy.client(), times(2)).describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(response.getResourceModel().getBlueGreenDeploymentIdentifier()).isEqualTo(blueGreenDeploymentIdentifier);
        assertThat(createCaptor.getValue().blueGreenDeploymentName()).isNotEmpty();
    }

    @Test
    public void handleRequest_createBlueGreenDeploymentWithBlueGreenDeploymentName() {
        final String blueGreenDeploymentIdentifier = RandomStringUtils.randomAlphabetic(32);
        final String blueGreenDeploymentName = RandomStringUtils.randomAlphabetic(32);

        when(rdsProxy.client().createBlueGreenDeployment(any(CreateBlueGreenDeploymentRequest.class)))
                .thenReturn(CreateBlueGreenDeploymentResponse.builder()
                        .blueGreenDeployment(BlueGreenDeployment.builder()
                                .blueGreenDeploymentIdentifier(blueGreenDeploymentIdentifier)
                                .build())
                        .build());

        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class)))
                .thenReturn(DescribeDbInstancesResponse.builder()
                        .dbInstances(DBInstance.builder()
                                .dbInstanceStatus("available")
                                .build())
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                () -> BlueGreenDeployment.builder()
                        .status("available")
                        .blueGreenDeploymentIdentifier(blueGreenDeploymentIdentifier)
                        .target("target-db-instance-identifier")
                        .source("source-db-instance-identifier")
                        .build(),
                () -> ResourceModel.builder()
                        .blueGreenDeploymentName(blueGreenDeploymentName)
                        .build(),
                expectSuccess()
        );

        ArgumentCaptor<CreateBlueGreenDeploymentRequest> createCaptor = ArgumentCaptor.forClass(CreateBlueGreenDeploymentRequest.class);
        verify(rdsProxy.client(), times(1)).createBlueGreenDeployment(createCaptor.capture());
        verify(rdsProxy.client(), times(2)).describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class));
        verify(rdsProxy.client(), times(2)).describeDBInstances(any(DescribeDbInstancesRequest.class));

        assertThat(response.getResourceModel().getBlueGreenDeploymentIdentifier()).isEqualTo(blueGreenDeploymentIdentifier);
        assertThat(createCaptor.getValue().blueGreenDeploymentName()).isEqualTo(blueGreenDeploymentName);
    }
}
