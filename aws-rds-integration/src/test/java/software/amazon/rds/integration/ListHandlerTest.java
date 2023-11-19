package software.amazon.rds.integration;

import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsRequest;
import software.amazon.awssdk.services.rds.model.DescribeIntegrationsResponse;
import software.amazon.awssdk.services.rds.model.Integration;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.common.error.ErrorCode;
import software.amazon.rds.test.common.core.HandlerName;

import java.time.Duration;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    RdsClient rdsClient;

    @Getter
    private ListHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.LIST;
    }

    private boolean expectServiceInvocation;

    @BeforeEach
    public void setup() {
        handler = new ListHandler(TEST_HANDLER_CONFIG);
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
    public void handleRequest_Success() {
        when(rdsProxy.client().describeIntegrations(any(DescribeIntegrationsRequest.class)))
                .thenReturn(DescribeIntegrationsResponse.builder()
                        .integrations(INTEGRATION_ACTIVE)
                        .marker("marker2")
                        .build());

        expectServiceInvocation = false;

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> INTEGRATION_ACTIVE_MODEL,
                expectSuccess()
        );

        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).containsExactly(Translator.translateToModel(INTEGRATION_ACTIVE));
        assertThat(response.getNextToken()).isEqualTo("marker2");

        verify(rdsProxy.client(), times(1)).describeIntegrations(any(DescribeIntegrationsRequest.class));
    }

    @Test
    public void handleRequest_onException_followsDefaultErrorChain() {
        when(rdsProxy.client().describeIntegrations(any(DescribeIntegrationsRequest.class)))
                .thenAnswer((a) -> AwsServiceException.builder()
                        .awsErrorDetails(AwsErrorDetails.builder()
                                .errorCode(ErrorCode.InternalFailure.toString())
                                .build())
                        .build());

        expectServiceInvocation = false;

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> INTEGRATION_ACTIVE_MODEL,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client(), times(1)).describeIntegrations(any(DescribeIntegrationsRequest.class));
    }

    @Override
    protected void expectResourceSupply(Supplier<Integration> supplier) {
        // not used in this test
        throw new UnsupportedOperationException();
    }
}
