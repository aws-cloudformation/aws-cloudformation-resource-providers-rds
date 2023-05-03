package software.amazon.rds.bluegreendeployment;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

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
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;
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
        handler = new ReadHandler(
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
    public void handleRequest_ReadBlueGreenDeployment() {
        test_handleRequest_base(
                new CallbackContext(),
                () -> BlueGreenDeployment.builder()
                        .build(),
                () -> ResourceModel.builder().build(),
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).describeBlueGreenDeployments(any(DescribeBlueGreenDeploymentsRequest.class));
    }
}
