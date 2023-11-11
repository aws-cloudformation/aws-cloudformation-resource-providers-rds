package software.amazon.rds.dbsnapshot;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.services.rds.model.CopyDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.CreateDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.DeleteDbSnapshotRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbSnapshotsRequest;
import software.amazon.awssdk.services.rds.model.ModifyDbSnapshotRequest;
import software.amazon.rds.common.handler.Tagging;

public class Translator {

    static CreateDbSnapshotRequest createDBSnapshotRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        return CreateDbSnapshotRequest.builder()
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .dbInstanceIdentifier(model.getDBInstanceIdentifier())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .build();
    }

    static CopyDbSnapshotRequest copyDBSnapshotRequest(
            final ResourceModel model,
            final Tagging.TagSet tagSet
    ) {
        return CopyDbSnapshotRequest.builder()
                .copyOptionGroup(model.getCopyOptionGroup())
                .copyTags(model.getCopyTags())
                .kmsKeyId(model.getKmsKeyId())
                .optionGroupName(model.getOptionGroupName())
                .sourceDBSnapshotIdentifier(model.getSourceDBSnapshotIdentifier())
                .sourceRegion(model.getSourceRegion())
                .tags(Tagging.translateTagsToSdk(tagSet))
                .targetCustomAvailabilityZone(model.getTargetCustomAvailabilityZone())
                .targetDBSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .build();
    }

    static ModifyDbSnapshotRequest modifyDBSnapshotRequest(
            final ResourceModel model
    ) {
        return ModifyDbSnapshotRequest.builder()
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .engineVersion(model.getEngineVersion())
                .optionGroupName(model.getOptionGroupName())
                .build();
    }

    static DescribeDbSnapshotsRequest describeDBSnapshotRequest(final ResourceModel model) {
        return DescribeDbSnapshotsRequest.builder()
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .build();
    }

    static DescribeDbSnapshotsRequest describeDBSnapshotRequest(final String nextToken) {
        return DescribeDbSnapshotsRequest.builder()
                .marker(nextToken)
                .build();
    }

    static DeleteDbSnapshotRequest deleteDBSnapshotRequest(final ResourceModel model) {
        return DeleteDbSnapshotRequest.builder()
                .dbSnapshotIdentifier(model.getDBSnapshotIdentifier())
                .build();
    }

    @VisibleForTesting
    static List<ProcessorFeature> translateProcessorFeaturesFromSdk(
            final List<software.amazon.awssdk.services.rds.model.ProcessorFeature> processorFeatures
    ) {
        if (processorFeatures == null) {
            return null;
        }

        return processorFeatures.stream()
                .map(processorFeature -> ProcessorFeature.builder()
                        .name(processorFeature.name())
                        .value(processorFeature.value())
                        .build())
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    static List<Tag> translateTagsFromSdk(
            final List<software.amazon.awssdk.services.rds.model.Tag> tags
    ) {
        if (tags == null) {
            return null;
        }

        return tags.stream()
                .map(tag -> Tag.builder()
                        .key(tag.key())
                        .value(tag.value())
                        .build())
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    static Set<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(
            final List<Tag> tags
    ) {
        if (tags == null) {
            return Collections.emptySet();
        }

        return tags.stream()
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build())
                .collect(Collectors.toSet());
    }

    @VisibleForTesting
    static ResourceModel translateDBSnapshotFromSdk(
            final software.amazon.awssdk.services.rds.model.DBSnapshot dbSnapshot
    ) {
        final String instanceCreateTime = dbSnapshot.instanceCreateTime() == null ? null : dbSnapshot.instanceCreateTime().toString();
        final String originalSnapshotCreateTime = dbSnapshot.originalSnapshotCreateTime() == null ? null : dbSnapshot.originalSnapshotCreateTime().toString();
        final String snapshotCreateTime = dbSnapshot.snapshotCreateTime() == null ? null : dbSnapshot.snapshotCreateTime().toString();
        final String snapshotDatabaseTime = dbSnapshot.snapshotDatabaseTime() == null ? null : dbSnapshot.snapshotDatabaseTime().toString();

        return ResourceModel.builder()
                .allocatedStorage(dbSnapshot.allocatedStorage())
                .availabilityZone(dbSnapshot.availabilityZone())
                .dBInstanceIdentifier(dbSnapshot.dbInstanceIdentifier())
                .dbiResourceId(dbSnapshot.dbiResourceId())
                .dBSnapshotArn(dbSnapshot.dbSnapshotArn())
                .dBSnapshotIdentifier(dbSnapshot.dbSnapshotIdentifier())
                .encrypted(dbSnapshot.encrypted())
                .engine(dbSnapshot.engine())
                .engineVersion(dbSnapshot.engineVersion())
                .iops(dbSnapshot.iops())
                .iAMDatabaseAuthenticationEnabled(dbSnapshot.iamDatabaseAuthenticationEnabled())
                .kmsKeyId(dbSnapshot.kmsKeyId())
                .instanceCreateTime(instanceCreateTime)
                .licenseModel(dbSnapshot.licenseModel())
                .masterUsername(dbSnapshot.masterUsername())
                .optionGroupName(dbSnapshot.optionGroupName())
                .originalSnapshotCreateTime(originalSnapshotCreateTime)
                .port(dbSnapshot.port())
                .percentProgress(dbSnapshot.percentProgress())
                .processorFeatures(translateProcessorFeaturesFromSdk(dbSnapshot.processorFeatures()))
                .snapshotCreateTime(snapshotCreateTime)
                .snapshotDatabaseTime(snapshotDatabaseTime)
                .snapshotTarget(dbSnapshot.snapshotTarget())
                .snapshotType(dbSnapshot.snapshotType())
                .sourceDBSnapshotIdentifier(dbSnapshot.sourceDBSnapshotIdentifier())
                .sourceRegion(dbSnapshot.sourceRegion())
                .status(dbSnapshot.status())
                .storageType(dbSnapshot.storageType())
                .tags(translateTagsFromSdk(dbSnapshot.tagList()))
                .storageThroughput(dbSnapshot.storageThroughput())
                .tdeCredentialArn(dbSnapshot.tdeCredentialArn())
                .timezone(dbSnapshot.timezone())
                .vpcId(dbSnapshot.vpcId())
                .build();
    }
}
