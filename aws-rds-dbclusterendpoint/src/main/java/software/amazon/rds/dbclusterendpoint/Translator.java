package software.amazon.rds.dbclusterendpoint;

import software.amazon.awssdk.services.rds.model.CreateDbClusterEndpointRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbClusterEndpointRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbClusterEndpointsRequest;
import software.amazon.rds.common.handler.Tagging;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Translator {

    static CreateDbClusterEndpointRequest createDbClusterEndpointRequest(
            final ResourceModel model,
            final Tagging.TagSet tags
    ) {
        return CreateDbClusterEndpointRequest.builder()
                .dbClusterEndpointIdentifier(model.getDBClusterEndpointIdentifier())
                .dbClusterIdentifier(model.getDBClusterIdentifier())
                .endpointType(model.getEndpointType())
                .staticMembers(model.getStaticMembers())
                .excludedMembers(model.getExcludedMembers())
                .tags(Tagging.translateTagsToSdk(tags))
                .build();
    }

    static DescribeDbClusterEndpointsRequest describeDbClustersEndpointRequest(final ResourceModel model) {
        return DescribeDbClusterEndpointsRequest.builder()
                .dbClusterEndpointIdentifier(model.getDBClusterEndpointIdentifier())
                .build();
    }

    static DescribeDbClusterEndpointsRequest describeDbClustersEndpointRequest(final String nextToken) {
        return DescribeDbClusterEndpointsRequest.builder()
                .marker(nextToken)
                .build();
    }

    static ResourceModel translateDbClusterEndpointFromSdk(
            final software.amazon.awssdk.services.rds.model.DBClusterEndpoint dbClusterEndpoint) {
        return ResourceModel.builder()
                .dBClusterEndpointIdentifier(dbClusterEndpoint.dbClusterEndpointIdentifier())
                .dBClusterIdentifier(dbClusterEndpoint.dbClusterIdentifier())
                .endpointType(dbClusterEndpoint.customEndpointType())
                .staticMembers(new HashSet<>(dbClusterEndpoint.staticMembers()))
                .excludedMembers(new HashSet<>(dbClusterEndpoint.excludedMembers()))
                .build();
    }

    public static List<ResourceModel> translateDbClusterEndpointFromSdk(
            final List<software.amazon.awssdk.services.rds.model.DBClusterEndpoint> dbInstances
    ) {
        return streamOfOrEmpty(dbInstances)
                .map(Translator::translateDbClusterEndpointFromSdk)
                .collect(Collectors.toList());
    }

    static DeleteDbClusterEndpointRequest deleteDbClusterEndpointRequest(final ResourceModel model) {
        return DeleteDbClusterEndpointRequest.builder()
                .dbClusterEndpointIdentifier(model.getDBClusterEndpointIdentifier())
                .build();
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }

    private static void addToMapIfAbsent(Map<String, software.amazon.awssdk.services.rds.model.Tag> allTags, Collection<software.amazon.awssdk.services.rds.model.Tag> tags) {
        for (software.amazon.awssdk.services.rds.model.Tag tag : tags) {
            allTags.putIfAbsent(tag.key(), tag);
        }
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
}
