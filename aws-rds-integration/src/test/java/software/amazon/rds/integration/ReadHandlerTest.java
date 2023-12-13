package software.amazon.rds.integration;

import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.test.common.core.HandlerName;

import java.time.Duration;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
public class ReadHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    RdsClient rdsClient;

    @Getter
    private ReadHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.READ;
    }

    @BeforeEach
    public void setup() {
        handler = new ReadHandler(TEST_HANDLER_CONFIG);
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
    void handleRequest_ReadSuccess() {
        test_handleRequest_base(
                new CallbackContext(),
                () -> INTEGRATION_ACTIVE,
                () -> INTEGRATION_ACTIVE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1))
                .describeIntegrations(
                        Mockito.<DescribeIntegrationsRequest>argThat(
                                req -> INTEGRATION_ARN.equals(req.integrationIdentifier()))
                );
    }

    @Test
    void handleRequest_ReadFailure() {
        test_handleRequest_base(
                new CallbackContext(),
                () -> { throw makeAwsServiceException(ErrorCode.InternalFailure); },
                () -> INTEGRATION_ACTIVE_MODEL,
                expectFailed(HandlerErrorCode.ServiceInternalError)
        );

        verify(rdsProxy.client(), times(1))
                .describeIntegrations(
                        Mockito.<DescribeIntegrationsRequest>argThat(
                                req -> INTEGRATION_ARN.equals(req.integrationIdentifier()))
                );
    }
}
