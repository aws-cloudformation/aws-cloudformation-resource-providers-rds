package software.amazon.rds.dbsnapshot;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import software.amazon.awssdk.services.rds.model.CreateDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.DBSnapshot;
import software.amazon.awssdk.services.rds.model.DeleteDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbSnapshotRequest;
import software.amazon.rds.common.handler.Tagging;

public class Translator {
    public static DescribeDbSnapshotsRequest describeDbSnapshotsRequest(final ResourceModel model) {
        return DescribeDbSnapshotsRequest.builder()
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .build();
    }

    public static DescribeDbSnapshotsRequest describeDbSnapshotsRequest(final String nextToken) {
        return DescribeDbSnapshotsRequest.builder()
                .marker(nextToken)
                .build();
    }

    public static Set<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final List<Tag> tags) {
        return streamOfOrEmpty(tags)
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build()
                )
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public static CreateDbSnapshotRequest createDbSnapshotRequest(final ResourceModel model,
                                                                  final Tagging.TagSet tags) {
        return CreateDbSnapshotRequest.builder()
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .tags(Tagging.translateTagsToSdk(tags))
                .build();
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
    }

    public static List<Tag> translateToModel(final Collection<software.amazon.awssdk.services.rds.model.Tag> sdkTags) {
        return streamOfOrEmpty(sdkTags)
                .map(tag -> Tag
                        .builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build()
                )
                .collect(Collectors.toList());
    }

    public static ResourceModel translateToModel(DBSnapshot dbSnapshot) {
        return ResourceModel.builder()
                .dBSnapshotIdentifier(dbSnapshot.dbSnapshotIdentifier())
                .dBInstanceIdentifier(dbSnapshot.dbInstanceIdentifier())
                .dBSnapshotArn(dbSnapshot.dbSnapshotArn())
                .engineVersion(dbSnapshot.engineVersion())
                .optionGroupName(dbSnapshot.optionGroupName())
                .tags(translateToModel(dbSnapshot.tagList()))
                .build();
    }

    public static DeleteDbSnapshotRequest deleteDbSnapshotRequest(final ResourceModel resourceModel) {
        return DeleteDbSnapshotRequest.builder()
                .dbSnapshotIdentifier(resourceModel.getDBSnapshotIdentifier())
                .build();
    }

    public static ModifyDbSnapshotRequest modifyDbDbSnapshotRequest(final ResourceModel dbSnapshot) {
        return ModifyDbSnapshotRequest.builder()
                .dbSnapshotIdentifier(dbSnapshot.getDBSnapshotIdentifier())
                .engineVersion(dbSnapshot.getEngineVersion())
                .optionGroupName(dbSnapshot.getOptionGroupName())
                .build();
    }
}
