package software.amazon.rds.dbsecuritygroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Collections;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSecurityGroup;
import software.amazon.awssdk.services.rds.model.DescribeDbSecurityGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSecurityGroupsResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    private RdsClient rdsClient;

    private ListHandler handler;

    final private String DB_SECURITY_GROUP_NAME = "test-db-security-group-name";
    final private String DB_SECURITY_GROUP_MARKER = "test-db-security-group-marker";

    @BeforeEach
    public void setup() {
        handler = new ListHandler();
        rdsClient = mock(RdsClient.class);
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        proxyClient = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final DescribeDbSecurityGroupsResponse describeDbSecurityGroupsResponse = DescribeDbSecurityGroupsResponse.builder()
                .dbSecurityGroups(
                        Collections.singletonList(
                                DBSecurityGroup.builder()
                                        .dbSecurityGroupName(DB_SECURITY_GROUP_NAME)
                                        .build()
                        )
                )
                .marker(DB_SECURITY_GROUP_MARKER)
                .build();
        when(proxyClient.client().describeDBSecurityGroups(any(DescribeDbSecurityGroupsRequest.class)))
                .thenReturn(describeDbSecurityGroupsResponse);

        final ResourceModel model = ResourceModel.builder().build();

        final ResourceModel expectedModel = ResourceModel.builder()
                .groupName(DB_SECURITY_GROUP_NAME)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(model)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackContext()).isNull();
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).containsExactly(expectedModel);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        assertThat(response.getNextToken()).isEqualTo(DB_SECURITY_GROUP_MARKER);

        verify(proxyClient.client()).describeDBSecurityGroups(any(DescribeDbSecurityGroupsRequest.class));
    }
}
