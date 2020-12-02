package software.amazon.rds.dbsecuritygroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AuthorizeDbSecurityGroupIngressRequest;
import software.amazon.awssdk.services.rds.model.AuthorizeDbSecurityGroupIngressResponse;
import software.amazon.awssdk.services.rds.model.CreateDbSecurityGroupRequest;
import software.amazon.awssdk.services.rds.model.CreateDbSecurityGroupResponse;
import software.amazon.awssdk.services.rds.model.DBSecurityGroup;
import software.amazon.awssdk.services.rds.model.DbSecurityGroupAlreadyExistsException;
import software.amazon.awssdk.services.rds.model.DescribeDbSecurityGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSecurityGroupsResponse;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class CreateHandlerTest extends AbstractTestBase {

    @Mock
    private AmazonWebServicesClientProxy proxy;

    @Mock
    private ProxyClient<RdsClient> proxyClient;

    @Mock
    RdsClient rdsClient;

    private CreateHandler handler;

    @BeforeEach
    public void setup() {
        handler = new CreateHandler();
        proxy = new AmazonWebServicesClientProxy(logger, MOCK_CREDENTIALS, () -> Duration.ofSeconds(600).toMillis());
        rdsClient = mock(RdsClient.class);
        proxyClient = MOCK_PROXY(proxy, rdsClient);
    }

    @AfterEach
    public void tear_down() {
        verify(rdsClient, atLeastOnce()).serviceName();
        verifyNoMoreInteractions(rdsClient);
    }

    @Test
    public void handleRequest_SimpleSuccess() {
        final DBSecurityGroup dbSecurityGroup = DB_SECURITY_GROUP.toBuilder()
                .ec2SecurityGroups(
                        DB_SECURITY_GROUP_INGRESSES.stream()
                                .map(Translator::translateIngresToEc2SecurityGroup)
                                .map(group -> group.toBuilder().status(IngressStatus.Authorized.toString()).build())
                                .collect(Collectors.toList())
                )
                .build();
        final CreateDbSecurityGroupResponse createDbSecurityGroupResponse = CreateDbSecurityGroupResponse.builder()
                .dbSecurityGroup(dbSecurityGroup)
                .build();
        when(proxyClient.client().createDBSecurityGroup(any(CreateDbSecurityGroupRequest.class)))
                .thenReturn(createDbSecurityGroupResponse);
        final DescribeDbSecurityGroupsResponse describeDbSecurityGroupsResponse = DescribeDbSecurityGroupsResponse.builder()
                .dbSecurityGroups(dbSecurityGroup)
                .build();
        when(proxyClient.client().describeDBSecurityGroups(any(DescribeDbSecurityGroupsRequest.class)))
                .thenReturn(describeDbSecurityGroupsResponse);
        final AuthorizeDbSecurityGroupIngressResponse authorizeDbSecurityGroupIngressResponse = AuthorizeDbSecurityGroupIngressResponse.builder()
                .build();
        when(proxyClient.client().authorizeDBSecurityGroupIngress(any(AuthorizeDbSecurityGroupIngressRequest.class)))
                .thenReturn(authorizeDbSecurityGroupIngressResponse);
        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tagList(Translator.translateTagsToSdk(TAG_SET))
                .build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(listTagsForResourceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1))
                .createDBSecurityGroup(any(CreateDbSecurityGroupRequest.class));
        verify(proxyClient.client(), times(RESOURCE_MODEL.getDBSecurityGroupIngress().size()))
                .authorizeDBSecurityGroupIngress(any(AuthorizeDbSecurityGroupIngressRequest.class));
        verify(proxyClient.client(), times(1 + RESOURCE_MODEL.getDBSecurityGroupIngress().size() + 1))
                .describeDBSecurityGroups(any(DescribeDbSecurityGroupsRequest.class));
    }

    @Test
    public void handleRequest_EmptyName() {
        final DBSecurityGroup dbSecurityGroup = DB_SECURITY_GROUP.toBuilder()
                .ec2SecurityGroups(
                        DB_SECURITY_GROUP_INGRESSES.stream()
                                .map(Translator::translateIngresToEc2SecurityGroup)
                                .map(group -> group.toBuilder().status(IngressStatus.Authorized.toString()).build())
                                .collect(Collectors.toList())
                )
                .build();
        final CreateDbSecurityGroupResponse createDbSecurityGroupResponse = CreateDbSecurityGroupResponse.builder()
                .dbSecurityGroup(dbSecurityGroup)
                .build();
        when(proxyClient.client().createDBSecurityGroup(any(CreateDbSecurityGroupRequest.class)))
                .thenReturn(createDbSecurityGroupResponse);
        final DescribeDbSecurityGroupsResponse describeDbSecurityGroupsResponse = DescribeDbSecurityGroupsResponse.builder()
                .dbSecurityGroups(dbSecurityGroup)
                .build();
        when(proxyClient.client().describeDBSecurityGroups(any(DescribeDbSecurityGroupsRequest.class)))
                .thenReturn(describeDbSecurityGroupsResponse);
        final AuthorizeDbSecurityGroupIngressResponse authorizeDbSecurityGroupIngressResponse = AuthorizeDbSecurityGroupIngressResponse.builder()
                .build();
        when(proxyClient.client().authorizeDBSecurityGroupIngress(any(AuthorizeDbSecurityGroupIngressRequest.class)))
                .thenReturn(authorizeDbSecurityGroupIngressResponse);
        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tagList(Translator.translateTagsToSdk(TAG_SET))
                .build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(listTagsForResourceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_NO_NAME)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getErrorCode()).isNull();

        verify(proxyClient.client(), times(1))
                .createDBSecurityGroup(any(CreateDbSecurityGroupRequest.class));
        verify(proxyClient.client(), times(RESOURCE_MODEL.getDBSecurityGroupIngress().size()))
                .authorizeDBSecurityGroupIngress(any(AuthorizeDbSecurityGroupIngressRequest.class));
        verify(proxyClient.client(), times(1 + RESOURCE_MODEL.getDBSecurityGroupIngress().size() + 1))
                .describeDBSecurityGroups(any(DescribeDbSecurityGroupsRequest.class));
    }

    @Test
    public void handleRequest_AlreadyExists() {
        final DbSecurityGroupAlreadyExistsException exception = DbSecurityGroupAlreadyExistsException.builder().build();
        when(proxyClient.client().createDBSecurityGroup(any(CreateDbSecurityGroupRequest.class))).thenThrow(exception);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.AlreadyExists);

        verify(proxyClient.client(), times(1))
                .createDBSecurityGroup(any(CreateDbSecurityGroupRequest.class));
    }

    @Test
    public void handleRequest_OtherException() {
        final RuntimeException exception = new RuntimeException();
        when(proxyClient.client().createDBSecurityGroup(any(CreateDbSecurityGroupRequest.class))).thenThrow(exception);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.InternalFailure);
    }
}
