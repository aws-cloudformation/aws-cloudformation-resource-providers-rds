package software.amazon.rds.dbinstance;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import software.amazon.awssdk.services.rds.model.DescribeDbClustersRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbInstancesRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.utils.StringUtils;

public class BaseTranslator {

    public static DescribeDbInstancesRequest describeDbInstancesRequest(final ResourceModel model) {
        return DescribeDbInstancesRequest.builder()
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .build();
    }

    public static DescribeDbClustersRequest describeDbClustersRequest(final ResourceModel model) {
        return DescribeDbClustersRequest.builder()
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .build();
    }

    public static DescribeDbInstancesRequest describeDbInstancesRequest(final String nextToken) {
        return DescribeDbInstancesRequest.builder()
                .marker(nextToken)
                .build();
    }

    public static DescribeDbSnapshotsRequest describeDbSnapshotsRequest(final ResourceModel model) {
        return DescribeDbSnapshotsRequest.builder()
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .build();
    }

    public static Integer getAllocatedStorage(final ResourceModel model) {
        Integer allocatedStorage = null;
        if (StringUtils.isNotBlank(model.getAllocatedStorage())) {
            allocatedStorage = Integer.parseInt(model.getAllocatedStorage());
        }
        return allocatedStorage;
    }

    public static Integer translatePortToSdk(final String portStr) {
        if (StringUtils.isEmpty(portStr)) {
            return null;
        }
        //NumberFormatException
        return Integer.parseInt(portStr, 10);
    }

    public static String translatePortFromSdk(final Integer port) {
        if (port == null) {
            return null;
        }
        return port.toString();
    }

    public static boolean canUpdateAllocatedStorage(final String fromAllocatedStorage, final String toAllocatedStorage) {
        if (fromAllocatedStorage == null || toAllocatedStorage == null) {
            return true;
        }
        final int from, to;
        try {
            from = Integer.parseInt(fromAllocatedStorage);
            to = Integer.parseInt(toAllocatedStorage);
        } catch (NumberFormatException e) {
            return true;
        }
        return to >= from;
    }

    public static boolean canUpdateIops(final Integer fromIops, final Integer toIops) {
        return fromIops == null || toIops == null || toIops >= fromIops;
    }

    public static List<Tag> translateTagsFromSdk(final Collection<software.amazon.awssdk.services.rds.model.Tag> sdkTags) {
        return streamOfOrEmpty(sdkTags)
                .map(tag -> Tag
                        .builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build()
                )
                .collect(Collectors.toList());
    }

    public static Map<String, String> translateTagsToRequest(final Collection<Tag> tags) {
        return streamOfOrEmpty(tags)
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue, (v1, v2) -> v2, LinkedHashMap::new));
    }

    public static Set<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Collection<Tag> tags) {
        return streamOfOrEmpty(tags)
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build()
                )
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }
}
