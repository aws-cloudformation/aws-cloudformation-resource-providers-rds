package software.amazon.rds.common.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ArnHelperTests {
    @Test
    public void isValidArn_returnsFalseWhenNull() {
        assertThat(ArnHelper.isValidArn(null)).isFalse();
    }

    @Test
    public void isValidArn_returnsFalseWhenInvalid() {
        assertThat(ArnHelper.isValidArn("invalid")).isFalse();
    }

    @Test
    public void isValidArn_returnsTrueWhenValid() {
        assertThat(ArnHelper.isValidArn("arn:aws:rds:us-east-1:1234567890:cluster-snapshot:mysnapshot")).isTrue();
    }

    @Test
    public void getResourceType_returnsClusterSnapshot() {
        assertThat(ArnHelper.getResourceType("arn:aws:rds:us-east-1:1234567890:cluster-snapshot:mysnapshot")).isEqualTo(ArnHelper.ResourceType.DB_CLUSTER_SNAPSHOT);
    }

    @Test
    public void getResourceType_returnsNullForNonRdsService() {
        assertThat(ArnHelper.getResourceType("arn:aws:someservice:us-east-1:1234567890:cluster-snapshot:mysnapshot")).isNull();
    }

    @Test
    public void getResourceType_returnsInstanceSnapshot() {
        assertThat(ArnHelper.getResourceType("arn:aws:rds:us-east-1:1234567890:snapshot:mysnapshot")).isEqualTo(ArnHelper.ResourceType.DB_INSTANCE_SNAPSHOT);
    }

    @Test
    public void getRegionFromArn_returnsRegion() {
        assertThat(ArnHelper.getRegionFromArn("arn:aws:rds:us-east-1:1234567890:snapshot:mysnapshot")).isEqualTo("us-east-1");
    }

    @Test
    public void getResourceName_returnsResourceName() {
        assertThat(ArnHelper.getResourceNameFromArn("arn:aws:rds:us-east-1:1234567890:snapshot:mysnapshot")).isEqualTo("mysnapshot");
    }
}
