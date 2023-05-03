package software.amazon.rds.bluegreendeployment;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.BlueGreenDeployment;
import software.amazon.awssdk.services.rds.model.DescribeBlueGreenDeploymentsRequest;
import software.amazon.awssdk.services.rds.model.SwitchoverBlueGreenDeploymentRequest;
import software.amazon.awssdk.services.rds.model.SwitchoverBlueGreenDeploymentResponse;
import software.amazon.cloudformation.exceptions.CfnInvalidRequestException;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractHandlerTest {
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
    private UpdateHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.UPDATE;
    }

    private boolean expectServiceInvocation;

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler(
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
        expectServiceInvocation = true;
    }

    @AfterEach
    public void tear_down() {
        if (expectServiceInvocation) {
            verify(rdsClient, atLeastOnce()).serviceName();
        }
        verifyNoMoreInteractions(rdsClient);
        verifyAccessPermissions(rdsClient);
    }

    @Test
    public void handleRequest_NoStageChangeThrowsInvalidRequestException() {
        expectServiceInvocation = false;

        assertThatThrownBy(() -> {
            test_handleRequest_base(
                    new CallbackContext(),
                    null,
                    () -> ResourceModel.builder()
                            .stage("blue")
                            .build(),
                    () -> ResourceModel.builder()
                            .stage("blue")
                            .build(),
                    expectFailed(HandlerErrorCode.InvalidRequest)
            );
        }).isInstanceOf(CfnInvalidRequestException.class);
    }

    @Test
    public void handleRequest_switchoverBlueGreenDeployment() {
        when(rdsProxy.client().switchoverBlueGreenDeployment(any(SwitchoverBlueGreenDeploymentRequest.class)))
                .thenReturn(SwitchoverBlueGreenDeploymentResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                () -> BlueGreenDeployment.builder()
                        .status("switchover_completed")
                        .build(),
                () -> ResourceModel.builder()
                        .blueGreenDeploymentIdentifier("blue-green-deployment-identifier")
                        .stage("blue")
                        .build(),
                () -> ResourceModel.builder()
                        .blueGreenDeploymentIdentifier("blue-green-deployment-identifier")
                        .stage("green")
                        .build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).switchoverBlueGreenDeployment(any(SwitchoverBlueGreenDeploymentRequest.class));
        verify(rdsProxy.client(), times(2)).describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class));
    }
}
