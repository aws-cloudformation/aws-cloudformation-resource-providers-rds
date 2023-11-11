package software.amazon.rds.dbsubnetgroup;


import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.mockito.internal.util.collections.Sets;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;
import software.amazon.cloudformation.proxy.delay.Constant;
import software.amazon.rds.test.common.core.HandlerName;
import software.amazon.rds.test.common.core.TestUtils;
import software.amazon.rds.test.common.verification.AccessPermissionVerificationMode;

public abstract class AbstractTestBase {
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final org.slf4j.Logger delegate;
    protected static final LoggerProxy logger;

    protected static final Constant TEST_BACKOFF_DELAY = Constant.of()
            .delay(Duration.ofSeconds(1L))
            .timeout(Duration.ofSeconds(10L))
            .build();

    protected static final ResourceModel RESOURCE_MODEL;
    protected static final ResourceModel RESOURCE_MODEL_ALTERNATIVE;
    protected static final DBSubnetGroup DB_SUBNET_GROUP_CREATING;
    protected static final DBSubnetGroup DB_SUBNET_GROUP_ACTIVE;
    protected static final Set<Tag> TAG_SET;

    static {
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.dateTimeFormat", "HH:mm:ss:SSS Z");
        MOCK_CREDENTIALS = new Credentials("accessKey", "secretKey", "token");

        delegate = LoggerFactory.getLogger("testing");
        logger = new LoggerProxy();

        RESOURCE_MODEL = ResourceModel.builder()
                .dBSubnetGroupName(null)
                .subnetIds(Lists.newArrayList("subnetId1", "subnetId2"))
                .dBSubnetGroupDescription("sample description")
                .build();

        RESOURCE_MODEL_ALTERNATIVE = ResourceModel.builder()
                .dBSubnetGroupName("db-subnetgroup")
                .subnetIds(Lists.newArrayList("subnetId1", "subnetId2"))
                .dBSubnetGroupDescription("sample description")
                .build();

        DB_SUBNET_GROUP_CREATING = DBSubnetGroup.builder()
                .subnetGroupStatus("Creating").build();

        DB_SUBNET_GROUP_ACTIVE = DBSubnetGroup.builder()
                .subnetGroupStatus("Complete").build();
        TAG_SET = Sets.newSet(Tag.builder().key("key").value("value").build());
    }

    public abstract HandlerName getHandlerName();

    private static final JSONObject resourceSchema = new Configuration().resourceSchemaJSONObject();

    public void verifyAccessPermissions(final Object mock) {
        new AccessPermissionVerificationMode()
                .withDefaultPermissions()
                .withSchemaPermissions(resourceSchema, getHandlerName())
                .verify(TestUtils.getVerificationData(mock));
    }

    static Map<String, String> translateTagsToMap(final Set<Tag> tags) {
        return tags.stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));

    }
}
