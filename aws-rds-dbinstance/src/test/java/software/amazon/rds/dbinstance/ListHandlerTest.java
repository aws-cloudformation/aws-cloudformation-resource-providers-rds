package software.amazon.rds.dbinstance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
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

import com.google.common.collect.ImmutableList;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
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
    private RdsClient rdsClient;

    @Mock
    private Ec2Client ec2Client;

    @Mock
    private ProxyClient<RdsClient> rdsProxyClient;

    @Mock
    private ProxyClient<Ec2Client> ec2ProxyClient;

    private ListHandler handler;

    final String DB_INSTANCE_IDENTIFIER = "test-db-instance-identifier";
    final String DESCRIBE_DB_INSTANCES_MARKER = "test-describe-db-instances-marker";

    @BeforeEach
    public void setup() {
        handler = new ListHandler();
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        ec2Client = mock(Ec2Client.class);
        rdsProxyClient = MOCK_PROXY(proxy, rdsClient);
        ec2ProxyClient = MOCK_PROXY(proxy, ec2Client);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyNoMoreInteractions(ec2Client);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final DescribeDbInstancesResponse describeDbInstanceResponse = DescribeDbInstancesResponse.builder()
                .dbInstances(Collections.singletonList(
                        DBInstance.builder()
                                .dbInstanceIdentifier(DB_INSTANCE_IDENTIFIER)
                                .build()
                ))
                .marker(DESCRIBE_DB_INSTANCES_MARKER)
                .build();
        when(rdsProxyClient.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeDbInstanceResponse);

        final ResourceModel expectedModel = ResourceModel.builder()
                .associatedRoles(Collections.emptyList())
                .enableCloudwatchLogsExports(Collections.emptyList())
                .processorFeatures(Collections.emptyList())
                .tags(Collections.emptyList())
                .dBSecurityGroups(Collections.emptyList())
                .vPCSecurityGroups(Collections.emptyList())
                .dBInstanceIdentifier(DB_INSTANCE_IDENTIFIER)
                .build();

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(ResourceModel.builder().build())
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), rdsProxyClient, ec2ProxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModel()).isNull();
        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).containsExactly(expectedModel);
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
        assertThat(response.getNextToken()).isEqualTo(DESCRIBE_DB_INSTANCES_MARKER);

        verify(rdsProxyClient.client()).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }
}
