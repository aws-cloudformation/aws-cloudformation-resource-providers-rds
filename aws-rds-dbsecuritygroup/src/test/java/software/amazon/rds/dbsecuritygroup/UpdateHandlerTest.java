package software.amazon.rds.dbsecuritygroup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceRequest;
import software.amazon.awssdk.services.rds.model.AddTagsToResourceResponse;
import software.amazon.awssdk.services.rds.model.AuthorizeDbSecurityGroupIngressRequest;
import software.amazon.awssdk.services.rds.model.AuthorizeDbSecurityGroupIngressResponse;
import software.amazon.awssdk.services.rds.model.DBSecurityGroup;
import software.amazon.awssdk.services.rds.model.DbSecurityGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.DescribeDbSecurityGroupsRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSecurityGroupsResponse;
import software.amazon.awssdk.services.rds.model.EC2SecurityGroup;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceRequest;
import software.amazon.awssdk.services.rds.model.ListTagsForResourceResponse;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceRequest;
import software.amazon.awssdk.services.rds.model.RemoveTagsFromResourceResponse;
import software.amazon.awssdk.services.rds.model.RevokeDbSecurityGroupIngressRequest;
import software.amazon.awssdk.services.rds.model.RevokeDbSecurityGroupIngressResponse;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.HandlerErrorCode;
import software.amazon.cloudformation.proxy.OperationStatus;
import software.amazon.cloudformation.proxy.ProgressEvent;
import software.amazon.cloudformation.proxy.ProxyClient;
import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

@ExtendWith(MockitoExtension.class)
public class UpdateHandlerTest extends AbstractTestBase {

    @Mock
    RdsClient rdsClient;
    @Mock
    private AmazonWebServicesClientProxy proxy;
    @Mock
    private ProxyClient<RdsClient> proxyClient;

    private UpdateHandler handler;

    @BeforeEach
    public void setup() {
        handler = new UpdateHandler();
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
    public void handleRequest_ModifyTags() {
        final DescribeDbSecurityGroupsResponse describeDbSecurityGroupsResponse = DescribeDbSecurityGroupsResponse.builder()
                .dbSecurityGroups(DB_SECURITY_GROUP)
                .build();
        when(proxyClient.client().describeDBSecurityGroups(any(DescribeDbSecurityGroupsRequest.class)))
                .thenReturn(describeDbSecurityGroupsResponse);
        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tagList(Translator.translateTagsToSdk(TAG_SET))
                .build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(listTagsForResourceResponse);
        final RemoveTagsFromResourceResponse removeTagsFromResourceResponse = RemoveTagsFromResourceResponse.builder().build();
        when(proxyClient.client().removeTagsFromResource(any(RemoveTagsFromResourceRequest.class)))
                .thenReturn(removeTagsFromResourceResponse);
        final AddTagsToResourceResponse addTagsToResourceResponse = AddTagsToResourceResponse.builder().build();
        when(proxyClient.client().addTagsToResource(any(AddTagsToResourceRequest.class)))
                .thenReturn(addTagsToResourceResponse);

        final CallbackContext callbackContext = new CallbackContext();
        callbackContext.setIngresModified(true);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL)
                .previousResourceState(RESOURCE_MODEL)
                .desiredResourceTags(Translator.translateTagsToRequest(TAG_SET_ALTER))
                .previousResourceTags(Translator.translateTagsToRequest(TAG_SET))
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, callbackContext, proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_ModifyIngres() {
        final List<EC2SecurityGroup> currentGroups = EC2_SECURITY_GROUPS
                .stream()
                .map(group -> group.toBuilder().build())
                .collect(Collectors.toList());
        final List<EC2SecurityGroup> desiredGroups = EC2_SECURITY_GROUPS_ALTER
                .stream()
                .map(group -> group.toBuilder()
                        .status(IngressStatus.Authorized.toString())
                        .build())
                .collect(Collectors.toList());

        final Queue<DBSecurityGroup> transitions = new ConcurrentLinkedQueue<>(
                computeEc2SecurityGroupTransitions(
                        DB_SECURITY_GROUP,
                        currentGroups,
                        desiredGroups
                )
        );
        transitions.add(DB_SECURITY_GROUP);
        transitions.add(DB_SECURITY_GROUP);

        when(proxyClient.client().describeDBSecurityGroups(any(DescribeDbSecurityGroupsRequest.class))).then(res -> {
            return DescribeDbSecurityGroupsResponse.builder()
                    .dbSecurityGroups(transitions.remove())
                    .build();
        });

        final AuthorizeDbSecurityGroupIngressResponse authorizeDbSecurityGroupIngressResponse = AuthorizeDbSecurityGroupIngressResponse.builder()
                .build();
        when(proxyClient.client().authorizeDBSecurityGroupIngress(any(AuthorizeDbSecurityGroupIngressRequest.class)))
                .thenReturn(authorizeDbSecurityGroupIngressResponse);
        final RevokeDbSecurityGroupIngressResponse revokeDbSecurityGroupIngressResponse = RevokeDbSecurityGroupIngressResponse.builder()
                .build();
        when(proxyClient.client().revokeDBSecurityGroupIngress(any(RevokeDbSecurityGroupIngressRequest.class)))
                .thenReturn(revokeDbSecurityGroupIngressResponse);

        final ListTagsForResourceResponse listTagsForResourceResponse = ListTagsForResourceResponse.builder()
                .tagList(Translator.translateTagsToSdk(TAG_SET))
                .build();
        when(proxyClient.client().listTagsForResource(any(ListTagsForResourceRequest.class)))
                .thenReturn(listTagsForResourceResponse);

        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .previousResourceState(RESOURCE_MODEL)
                .desiredResourceState(RESOURCE_MODEL_ALTER_INGRES)
                .desiredResourceTags(Translator.translateTagsToRequest(TAG_SET))
                .previousResourceTags(Translator.translateTagsToRequest(TAG_SET))
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();

        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.SUCCESS);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
        assertThat(response.getResourceModels()).isNull();
        assertThat(response.getMessage()).isNull();
        assertThat(response.getErrorCode()).isNull();
    }

    @Test
    public void handleRequest_NotFound() {
        final DbSecurityGroupNotFoundException exception = DbSecurityGroupNotFoundException.builder().build();
        when(proxyClient.client().describeDBSecurityGroups(any(DescribeDbSecurityGroupsRequest.class)))
                .thenThrow(exception);
        final ResourceHandlerRequest<ResourceModel> request = ResourceHandlerRequest.<ResourceModel>builder()
                .desiredResourceState(RESOURCE_MODEL_ALTER_INGRES)
                .logicalResourceIdentifier(LOGICAL_RESOURCE_IDENTIFIER)
                .clientRequestToken(getClientRequestToken())
                .stackId(getStackId())
                .build();
        final ProgressEvent<ResourceModel, CallbackContext> response = handler.handleRequest(proxy, request, new CallbackContext(), proxyClient, logger);

        assertThat(response).isNotNull();
        assertThat(response.getStatus()).isEqualTo(OperationStatus.FAILED);
        assertThat(response.getErrorCode()).isEqualTo(HandlerErrorCode.NotFound);
        assertThat(response.getCallbackDelaySeconds()).isEqualTo(0);
    }

    private List<DBSecurityGroup> computeEc2SecurityGroupTransitions(
            final DBSecurityGroup dbSecurityGroup,
            final List<EC2SecurityGroup> initialGroups,
            final List<EC2SecurityGroup> finalGroups
    ) {
        final List<DBSecurityGroup> result = new LinkedList<>();

        for (int i = 0; i < initialGroups.size(); i++) {
            result.add(
                    dbSecurityGroup.toBuilder()
                            .ec2SecurityGroups(
                                    initialGroups.subList(i, initialGroups.size())
                            )
                            .build()
            );
        }

        for (int i = 0; i <= finalGroups.size(); i++) {
            result.add(
                    dbSecurityGroup.toBuilder()
                            .ec2SecurityGroups(
                                    finalGroups.subList(0, i)
                            )
                            .build()
            );
        }

        return result;
    }
}
