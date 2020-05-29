package software.amazon.rds.dbsubnetgroup;

import org.junit.jupiter.api.AfterEach;
import software.amazon.awssdk.services.rds.RdsClient;

import software.amazon.awssdk.services.rds.model.DbSubnetGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DeleteDBSubnetGroupResponse;
import software.amazon.awssdk.services.rds.model.DeleteDbSubnetGroupRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSubnetGroupsRequest;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;
import software.amazon.cloudformation.proxy.OperationStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DeleteHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyRdsClient;

    @Mock
    RdsClient rds;

    private DeleteHandler handler;

    @BeforeEach
    public void setup() {
        handler = new DeleteHandler();
        rds = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyRdsClient = MOCK_PROXY(proxy, rds);
    }

    @AfterEach
    public void post_execute() {
        verify(rds, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(proxyRdsClient.client());
    }

    @Test
    public void handleRequest_SimpleSuccess() {

        final DeleteDBSubnetGroupResponse deleteDBSubnetGroupResponse = DeleteDBSubnetGroupResponse.builder().build();
        when(proxyRdsClient.client().deleteDBSubnetGroup(any(DeleteDbSubnetGroupRequest.class))).thenReturn(deleteDBSubnetGroupResponse);
        when(proxyRdsClient.client().describeDBSubnetGroups(any(DescribeDbSubnetGroupsRequest.class))).thenThrow(DbSubnetGroupNotFoundException.class);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyRdsClient, logger);


        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyRdsClient.client()).deleteDBSubnetGroup(any(DeleteDbSubnetGroupRequest.class));
        verify(proxyRdsClient.client()).describeDBSubnetGroups(any(DescribeDbSubnetGroupsRequest.class));
    }
}
