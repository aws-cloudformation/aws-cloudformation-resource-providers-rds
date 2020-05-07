package software.amazon.rds.dbsubnetgroup;


import com.google.common.collect.Lists;
import software.amazon.awssdk.services.rds.model.DBCluster;
import software.amazon.awssdk.services.rds.model.DBSubnetGroup;
import software.amazon.cloudformation.proxy.Credentials;
import software.amazon.cloudformation.proxy.LoggerProxy;

import org.slf4j.LoggerFactory;

public class AbstractTestBase {
    protected static final Credentials MOCK_CREDENTIALS;
    protected static final org.slf4j.Logger delegate;
    protected static final LoggerProxy logger;



    protected static final ResourceModel RESOURCE_MODEL;
    protected static final ResourceModel RESOURCE_MODEL_ALTERNATIVE;
    protected static final DBSubnetGroup DB_SUBNET_GROUP_CREATING;
    protected static final DBSubnetGroup DB_SUBNET_GROUP_ACTIVE;

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
    }
}
