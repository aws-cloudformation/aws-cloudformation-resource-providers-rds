package software.amazon.rds.dbclusterparametergroup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.security.InvalidParameterException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import lombok.Getter;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterParameterGroupRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterParameterGroupResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.common.handler.HandlerConfig;
import software.amazon.rds.test.common.core.BaseProxyClient;
import software.amazon.rds.test.common.core.HandlerName;

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
    RdsClient rdsClient;

    @Getter
    private DeleteHandler handler;

    private ResourceModel RESOURCE_MODEL;

    private Map<String, Object> PARAMS;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.DELETE;
    }

    @BeforeEach
    public void setup() {

        handler = new DeleteHandler(HandlerConfig.builder()
                .backoff(Constant.of()
                        .delay(Duration.ofSeconds(1))
                        .timeout(Duration.ofSeconds(120))
                        .build())
                .build());
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsProxy = new BaseProxyClient<>(proxy, rdsClient);


        PARAMS = new HashMap<>();
        PARAMS.put("param", "value");
        PARAMS.put("param2", "value");

        RESOURCE_MODEL = ResourceModel.builder()
                .description(DESCRIPTION)
                .dBClusterParameterGroupName(null)
                .family(FAMILY)
                .parameters(PARAMS)
                .tags(TAG_SET)
                .build();
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsProxy.client());
        verifyAccessPermissions(rdsProxy.client());
    }

    @Test
    public void handleRequest_Success() {
        when(rdsClient.deleteDBClusterParameterGroup(any(DeleteDbClusterParameterGroupRequest.class)))
                .thenReturn(DeleteDbClusterParameterGroupResponse.builder().build());

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectSuccess()
        );

        verify(rdsProxy.client(), times(1)).deleteDBClusterParameterGroup(any(DeleteDbClusterParameterGroupRequest.class));
    }

    @Test
    public void handleRequest_NotFoundException() {
        DbParameterGroupNotFoundException dbParameterGroupNotFoundException = DbParameterGroupNotFoundException.builder()
                .awsErrorDetails(AwsErrorDetails.builder().build()).build();

        when(rdsClient.deleteDBClusterParameterGroup(any(DeleteDbClusterParameterGroupRequest.class)))
                .thenThrow(dbParameterGroupNotFoundException);

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.NotFound)
        );

        verify(rdsProxy.client()).deleteDBClusterParameterGroup(any(DeleteDbClusterParameterGroupRequest.class));
    }

    @Test
    public void handleRequest_InvalidParameterException() {
        when(rdsClient.deleteDBClusterParameterGroup(any(DeleteDbClusterParameterGroupRequest.class)))
                .thenThrow(new InvalidParameterException("Invalid parameter"));

        test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL,
                expectFailed(HandlerErrorCode.InternalFailure)
        );

        verify(rdsProxy.client()).deleteDBClusterParameterGroup(any(DeleteDbClusterParameterGroupRequest.class));
    }
}
