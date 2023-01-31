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

import lombok.Getter;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBInstance;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.rds.test.common.core.HandlerName;

@ExtendWith(MockitoExtension.class)
public class ListHandlerTest extends AbstractHandlerTest {

    final String DB_INSTANCE_IDENTIFIER = "test-db-instance-identifier";
    final String DESCRIBE_DB_INSTANCES_MARKER = "test-describe-db-instances-marker";
    @Mock
    @Getter
    private AmazonWebServicesClientProxy proxy;
    @Mock
    @Getter
    private ProxyClient<RdsClient> rdsProxy;
    @Mock
    @Getter
    private ProxyClient<Ec2Client> ec2Proxy;
    @Mock
    private RdsClient rdsClient;
    @Mock
    private Ec2Client ec2Client;
    @Getter
    private ListHandler handler;

    @Override
    public HandlerName getHandlerName() {
        return HandlerName.LIST;
    }

    @BeforeEach
    public void setup() {
        handler = new ListHandler();
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        ec2Client = mock(Ec2Client.class);
        rdsProxy = mockProxy(proxy, rdsClient);
        ec2Proxy = mockProxy(proxy, ec2Client);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
        verifyNoMoreInteractions(ec2Client);
        verifyAccessPermissions(rdsClient);
        verifyAccessPermissions(ec2Client);
    }

    @Test
    public void handleRequest_Success() {
        final DescribeDbInstancesResponse describeDbInstanceResponse = DescribeDbInstancesResponse.builder()
                .dbInstances(Collections.singletonList(
                        DBInstance.builder()
                                .dbInstanceIdentifier(DB_INSTANCE_IDENTIFIER)
                                .build()
                ))
                .marker(DESCRIBE_DB_INSTANCES_MARKER)
                .build();
        when(rdsProxy.client().describeDBInstances(any(DescribeDbInstancesRequest.class))).thenReturn(describeDbInstanceResponse);

        final ProgressEvent<ResourceModel, CallbackContext> response = test_handleRequest_base(
                new CallbackContext(),
                null,
                () -> RESOURCE_MODEL_BLDR().build(),
                expectSuccess()
        );

        final ResourceModel expectedModel = ResourceModel.builder()
                .associatedRoles(Collections.emptyList())
                .enableCloudwatchLogsExports(Collections.emptyList())
                .manageMasterUserPassword(false)
                .masterUserSecret(MasterUserSecret.builder().build())
                .processorFeatures(Collections.emptyList())
                .tags(Collections.emptyList())
                .dBSecurityGroups(Collections.emptyList())
                .vPCSecurityGroups(Collections.emptyList())
                .dBInstanceIdentifier(DB_INSTANCE_IDENTIFIER)
                .build();

        assertThat(response.getResourceModels()).isNotNull();
        assertThat(response.getResourceModels()).containsExactly(expectedModel);
        assertThat(response.getNextToken()).isEqualTo(DESCRIBE_DB_INSTANCES_MARKER);

        verify(rdsProxy.client()).describeDBInstances(any(DescribeDbInstancesRequest.class));
    }
}
