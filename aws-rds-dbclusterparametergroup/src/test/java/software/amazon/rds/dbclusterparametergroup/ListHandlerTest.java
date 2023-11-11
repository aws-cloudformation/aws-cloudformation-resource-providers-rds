package software.amazon.rds.dbclusterparametergroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
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
import software.amazon.awssdk.services.rds.model.DBClusterParameterGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterParameterGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.BaseProxyClient;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractHandlerTest {

    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;

    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;

    @Mock
    @Getter
    RdsClient rdsClient;

    @Getter
    private ListHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.LIST;
    }

    @BeforeEach
    public void setup() {
        handler = new ListHandler(HandlerConfig.builder()
                .backoff(Constant.of()
                        .delay(Duration.ofSeconds(1))
                        .timeout(Duration.ofSeconds(120))
                        .build())
                .build());

        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = new BaseProxyClient<>(proxy, rdsClient);
    }

    @AfterEach
    public void post_execute() {
        verifyNoMoreInteractions(rdsProxy.client());
        verifyAccessPermissions(rdsProxy.client());
    }

    @Test
    public void handleRequest_Success() {
        final String marker = "marker";

        when(rdsProxy.client().describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class)))
                .thenReturn(DescribeDbClusterParameterGroupsResponse.builder()
                        .dbClusterParameterGroups(DBClusterParameterGroup.builder().build())
                        .marker(marker)
                        .build());

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).containsExactly(Translator.translateFromSdk(DBClusterParameterGroup.builder().build()));
        assertThat(response.getNextToken()).isEqualTo(marker);

        verify(rdsProxy.client(), times(1)).describeDBClusterParameterGroups(any(DescribeDbClusterParameterGroupsRequest.class));
    }
}
