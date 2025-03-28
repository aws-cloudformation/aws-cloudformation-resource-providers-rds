package software.amazon.rds.common.util;

import com.amazonaws.arn.Arn;
import com.amazonaws.arn.ArnResource;
import com.amazonaws.util.StringUtils;


public final class ArnHelper {
    private static String RDS_SERVICE = "rds";
    public enum ResourceType {
        DB_INSTANCE_SNAPSHOT("snapshot"),
        DB_CLUSTER_SNAPSHOT("cluster-snapshot");

        private String value;

        ResourceType(String value) {
            this.value = value;
        }

        public static ResourceType fromString(final String resourceString) {
            if (!StringUtils.isNullOrEmpty(resourceString)) {
                for (final ResourceType type : ResourceType.values()) {
                    if (type.value.equals(resourceString)) {
                        return type;
                    }
                }
            }
            return null;
        }
    }

    public static boolean isValidArn(final String arn) {
        if (StringUtils.isNullOrEmpty(arn)) {
            return false;
        }
        try {
            Arn.fromString(arn);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    public static String getRegionFromArn(final String arn) {
        return Arn.fromString(arn).getRegion();
    }

    public static String getResourceNameFromArn(final String arn) {
        return Arn.fromString(arn).getResource().getResource();
    }

    public static String getAccountIdFromArn(final String arn) {
        return Arn.fromString(arn).getAccountId();
    }

    public static ResourceType getResourceType(String potentialArn) {
        if (isValidArn(potentialArn)) {
            final Arn arn = Arn.fromString(potentialArn);
            if (!RDS_SERVICE.equalsIgnoreCase(arn.getService())) {
                return null;
            }
            final ArnResource resource = arn.getResource();
            return ResourceType.fromString(resource.getResourceType());
        }
        return null;
    }
}
