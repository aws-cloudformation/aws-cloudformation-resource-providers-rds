package software.amazon.rds.dbsecuritygroup;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.awscore.AwsResponse;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.DBSecurityGroup;
import software.amazon.awssdk.services.rds.model.EC2SecurityGroup;
import software.amazon.cloudformation.proxy.AmazonWebServicesClientProxy;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.ProxyClient;

public class AbstractTestBase {
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final LoggerProxy logger;

    protected static final String LOGICAL_RESOURCE_IDENTIFIER = "db-parameter-group";

    protected static final String DB_SECURITY_GROUP_ARN = "db-security-group-arn";
    protected static final String DB_SECURITY_GROUP_NAME = "db-security-group-name";
    protected static final String DB_SECURITY_GROUP_DESCRIPTION = "db-security-group-description";
    protected static final String DB_SECURITY_GROUP_EC2_VPC_ID = "db-security-group-ec2-vpc-id";

    protected static final String EC2_SECURITY_GROUP_NAME_1 = "ec2-security-group-name-1";
    protected static final String EC2_SECURITY_GROUP_NAME_2 = "ec2-security-group-name-2";
    protected static final String EC2_SECURITY_GROUP_NAME_3 = "ec2-security-group-name-3";
    protected static final String EC2_SECURITY_GROUP_NAME_4 = "ec2-security-group-name-4";
    protected static final String EC2_SECURITY_GROUP_OWNER_ID_1 = "ec2-security-group-owner-id-1";
    protected static final String EC2_SECURITY_GROUP_OWNER_ID_2 = "ec2-security-group-owner-id-2";
    protected static final String EC2_SECURITY_GROUP_OWNER_ID_3 = "ec2-security-group-owner-id-3";
    protected static final String EC2_SECURITY_GROUP_OWNER_ID_4 = "ec2-security-group-owner-id-4";
    protected static final String EC2_SECURITY_GROUP_ID_1 = "ec2-security-group-id-1";
    protected static final String EC2_SECURITY_GROUP_ID_2 = "ec2-security-group-id-2";
    protected static final String EC2_SECURITY_GROUP_ID_3 = "ec2-security-group-id-3";
    protected static final String EC2_SECURITY_GROUP_ID_4 = "ec2-security-group-id-4";

    protected static final String EC2_SECURITY_GROUP_IP_RANGE = "192.0.2.0/24";

    protected static final List<Tag> TAG_SET;
    protected static final List<Tag> TAG_SET_EMPTY;
    protected static final List<Tag> TAG_SET_ALTER;

    protected static final List<Ingress> DB_SECURITY_GROUP_INGRESSES;
    protected static final List<Ingress> DB_SECURITY_GROUP_INGRESSES_EMPTY;
    protected static final List<Ingress> DB_SECURITY_GROUP_INGRESSES_ALTER;

    protected static final List<EC2SecurityGroup> EC2_SECURITY_GROUPS;
    protected static final List<EC2SecurityGroup> EC2_SECURITY_GROUPS_EMPTY;
    protected static final List<EC2SecurityGroup> EC2_SECURITY_GROUPS_ALTER;

    protected static final ResourceModel RESOURCE_MODEL;
    protected static final ResourceModel RESOURCE_MODEL_NO_NAME;
    protected static final ResourceModel RESOURCE_MODEL_EMPTY_INGRES;
    protected static final ResourceModel RESOURCE_MODEL_ALTER_INGRES;
    protected static final ResourceModel RESOURCE_MODEL_NO_TAGS;
    protected static final DBSecurityGroup DB_SECURITY_GROUP;

    static {
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");
        logger = new LoggerProxy();

        TAG_SET_EMPTY = ImmutableList.of();
        TAG_SET = ImmutableList.of(
                Tag.builder().key("tag-1").value("value-1").build(),
                Tag.builder().key("tag-2").value("value-2").build()
        );
        TAG_SET_ALTER = ImmutableList.of(
                Tag.builder().key("tag-2").value("value-2").build(),
                Tag.builder().key("tag-3").value("value-3").build()
        );

        DB_SECURITY_GROUP_INGRESSES = ImmutableList.of(
                Ingress.builder()
                        .eC2SecurityGroupName("ec2-security-group-name-1")
                        .eC2SecurityGroupId("ec2-security-group-id-1")
                        .eC2SecurityGroupOwnerId("ec2-security-group-owner-id-1")
                        .cIDRIP(EC2_SECURITY_GROUP_IP_RANGE)
                        .build(),
                Ingress.builder()
                        .eC2SecurityGroupName("ec2-security-group-name-2")
                        .eC2SecurityGroupId("ec2-security-group-id-2")
                        .eC2SecurityGroupOwnerId("ec2-security-group-owner-id-2")
                        .cIDRIP(EC2_SECURITY_GROUP_IP_RANGE)
                        .build()
        );
        DB_SECURITY_GROUP_INGRESSES_EMPTY = ImmutableList.of();
        DB_SECURITY_GROUP_INGRESSES_ALTER = ImmutableList.of(
                Ingress.builder()
                        .eC2SecurityGroupName("ec2-security-group-name-3")
                        .eC2SecurityGroupId("ec2-security-group-id-3")
                        .eC2SecurityGroupOwnerId("ec2-security-group-owner-id-3")
                        .cIDRIP(EC2_SECURITY_GROUP_IP_RANGE)
                        .build(),
                Ingress.builder()
                        .eC2SecurityGroupName("ec2-security-group-name-4")
                        .eC2SecurityGroupId("ec2-security-group-id-4")
                        .eC2SecurityGroupOwnerId("ec2-security-group-owner-id-4")
                        .cIDRIP(EC2_SECURITY_GROUP_IP_RANGE)
                        .build()
        );

        EC2_SECURITY_GROUPS = ImmutableList.of(
                EC2SecurityGroup.builder()
                        .ec2SecurityGroupId(EC2_SECURITY_GROUP_ID_1)
                        .ec2SecurityGroupName(EC2_SECURITY_GROUP_NAME_1)
                        .ec2SecurityGroupOwnerId(EC2_SECURITY_GROUP_OWNER_ID_1)
                        .build(),
                EC2SecurityGroup.builder()
                        .ec2SecurityGroupId(EC2_SECURITY_GROUP_ID_2)
                        .ec2SecurityGroupName(EC2_SECURITY_GROUP_NAME_2)
                        .ec2SecurityGroupOwnerId(EC2_SECURITY_GROUP_OWNER_ID_2)
                        .build()
        );
        EC2_SECURITY_GROUPS_EMPTY = ImmutableList.of();
        EC2_SECURITY_GROUPS_ALTER = ImmutableList.of(
                EC2SecurityGroup.builder()
                        .ec2SecurityGroupId(EC2_SECURITY_GROUP_ID_3)
                        .ec2SecurityGroupName(EC2_SECURITY_GROUP_NAME_3)
                        .ec2SecurityGroupOwnerId(EC2_SECURITY_GROUP_OWNER_ID_3)
                        .build(),
                EC2SecurityGroup.builder()
                        .ec2SecurityGroupId(EC2_SECURITY_GROUP_ID_4)
                        .ec2SecurityGroupName(EC2_SECURITY_GROUP_NAME_4)
                        .ec2SecurityGroupOwnerId(EC2_SECURITY_GROUP_OWNER_ID_4)
                        .build()
        );

        RESOURCE_MODEL = ResourceModel.builder()
                .groupName(DB_SECURITY_GROUP_NAME)
                .groupDescription(DB_SECURITY_GROUP_DESCRIPTION)
                .tags(TAG_SET)
                .dBSecurityGroupIngress(DB_SECURITY_GROUP_INGRESSES)
                .eC2VpcId(DB_SECURITY_GROUP_EC2_VPC_ID)
                .build();

        RESOURCE_MODEL_NO_NAME = ResourceModel.builder()
                .groupDescription(DB_SECURITY_GROUP_DESCRIPTION)
                .tags(TAG_SET)
                .dBSecurityGroupIngress(DB_SECURITY_GROUP_INGRESSES)
                .eC2VpcId(DB_SECURITY_GROUP_EC2_VPC_ID)
                .build();

        RESOURCE_MODEL_EMPTY_INGRES = ResourceModel.builder()
                .groupName(DB_SECURITY_GROUP_NAME)
                .groupDescription(DB_SECURITY_GROUP_DESCRIPTION)
                .tags(TAG_SET)
                .dBSecurityGroupIngress(DB_SECURITY_GROUP_INGRESSES_EMPTY)
                .eC2VpcId(DB_SECURITY_GROUP_EC2_VPC_ID)
                .build();

        RESOURCE_MODEL_ALTER_INGRES = ResourceModel.builder()
                .groupName(DB_SECURITY_GROUP_NAME)
                .groupDescription(DB_SECURITY_GROUP_DESCRIPTION)
                .tags(TAG_SET)
                .dBSecurityGroupIngress(DB_SECURITY_GROUP_INGRESSES_ALTER)
                .eC2VpcId(DB_SECURITY_GROUP_EC2_VPC_ID)
                .build();

        RESOURCE_MODEL_NO_TAGS = ResourceModel.builder()
                .groupName(DB_SECURITY_GROUP_NAME)
                .groupDescription(DB_SECURITY_GROUP_DESCRIPTION)
                .dBSecurityGroupIngress(DB_SECURITY_GROUP_INGRESSES)
                .eC2VpcId(DB_SECURITY_GROUP_EC2_VPC_ID)
                .build();

        DB_SECURITY_GROUP = DBSecurityGroup.builder()
                .dbSecurityGroupArn(DB_SECURITY_GROUP_ARN)
                .dbSecurityGroupName(DB_SECURITY_GROUP_NAME)
                .dbSecurityGroupDescription(DB_SECURITY_GROUP_DESCRIPTION)
                .build();
    }

    static ProxyClient<RdsClient> MOCK_PROXY(
            final AmazonWebServicesClientProxy proxy,
            final RdsClient rdsClient) {
        return new ProxyClient<RdsClient>() {
            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseT
            injectCredentialsAndInvokeV2(RequestT request, Function<RequestT, ResponseT> requestFunction) {
                return proxy.injectCredentialsAndInvokeV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse>
            CompletableFuture<ResponseT>
            injectCredentialsAndInvokeV2Async(RequestT request, Function<RequestT, CompletableFuture<ResponseT>> requestFunction) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse, IterableT extends SdkIterable<ResponseT>>
            IterableT
            injectCredentialsAndInvokeIterableV2(RequestT request, Function<RequestT, IterableT> requestFunction) {
                return proxy.injectCredentialsAndInvokeIterableV2(request, requestFunction);
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseInputStream<ResponseT>
            injectCredentialsAndInvokeV2InputStream(RequestT requestT, Function<RequestT, ResponseInputStream<ResponseT>> function) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <RequestT extends AwsRequest, ResponseT extends AwsResponse> ResponseBytes<ResponseT>
            injectCredentialsAndInvokeV2Bytes(RequestT requestT, Function<RequestT, ResponseBytes<ResponseT>> function) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RdsClient client() {
                return rdsClient;
            }
        };
    }

    static String getClientRequestToken() {
        return UUID.randomUUID().toString();
    }

    static String getStackId() {
        return UUID.randomUUID().toString();
    }
}
