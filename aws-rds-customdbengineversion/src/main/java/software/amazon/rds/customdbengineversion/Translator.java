package software.amazon.rds.customdbengineversion;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import software.amazon.awssdk.services.rds.model.CreateCustomDbEngineVersionRequest;
import software.amazon.awssdk.services.rds.model.DBEngineVersion;
import software.amazon.awssdk.services.rds.model.DeleteCustomDbEngineVersionRequest;
import software.amazon.awssdk.services.rds.model.DescribeDbEngineVersionsRequest;
import software.amazon.awssdk.services.rds.model.ModifyCustomDbEngineVersionRequest;
import software.amazon.rds.common.handler.Tagging;
import software.amazon.rds.common.util.DifferenceUtils;

public class Translator {
    public static CreateCustomDbEngineVersionRequest createCustomDbEngineVersionRequest(
            final ResourceModel model,
            final Tagging.TagSet tags
    ) {
        return CreateCustomDbEngineVersionRequest.builder()
                .databaseInstallationFilesS3BucketName(model.getDatabaseInstallationFilesS3BucketName())
                .databaseInstallationFilesS3Prefix(model.getDatabaseInstallationFilesS3Prefix())
                .description(model.getDescription())
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .kmsKeyId(model.getKMSKeyId())
                .manifest(model.getManifest())
                .tags(Tagging.translateTagsToSdk(tags))
                .build();
    }

    static DescribeDbEngineVersionsRequest describeDbEngineVersionsRequest(final ResourceModel model) {
        return DescribeDbEngineVersionsRequest.builder()
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .includeAll(true)
                .build();
    }

    static ModifyCustomDbEngineVersionRequest modifyCustomDbEngineVersionRequest(final ResourceModel previousModel,
                                                                                 final ResourceModel model) {
        return ModifyCustomDbEngineVersionRequest.builder()
                //Engine and EngineVersion together are the primary identifier of EngineVersion
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .description(DifferenceUtils.diff(previousModel != null ? previousModel.getDescription() : null, model.getDescription()))
                .status(DifferenceUtils.diff(previousModel != null ? previousModel.getStatus() : null, model.getStatus()))
                .build();
    }


    public static DeleteCustomDbEngineVersionRequest deleteCustomDbEngineVersion(final ResourceModel model) {
        return DeleteCustomDbEngineVersionRequest.builder()
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .build();
    }

    public static List<software.amazon.awssdk.services.rds.model.Tag> translateTagsToSdk(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyList())
                .stream()
                .map(tag -> software.amazon.awssdk.services.rds.model.Tag.builder()
                        .key(tag.getKey())
                        .value(tag.getValue())
                        .build()
                )
                .collect(Collectors.toList());
    }

    public static Map<String, String> translateTagsToRequest(final Collection<Tag> tags) {
        return Optional.ofNullable(tags).orElse(Collections.emptyList())
                .stream()
                .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
    }

    private static <T> Stream<T> streamOfOrEmpty(final Collection<T> collection) {
        return Optional.ofNullable(collection)
                .map(Collection::stream)
                .orElseGet(Stream::empty);
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

    static ResourceModel translateFromSdk(
            final DBEngineVersion engineVersion
    ) {
        return ResourceModel.builder()
                .dBEngineVersionArn(engineVersion.dbEngineVersionArn())
                .databaseInstallationFilesS3BucketName(engineVersion.databaseInstallationFilesS3BucketName())
                .databaseInstallationFilesS3Prefix(engineVersion.databaseInstallationFilesS3Prefix())
                .description(engineVersion.dbEngineVersionDescription())
                .engine(engineVersion.engine())
                .engineVersion(engineVersion.engineVersion())
                .kMSKeyId(engineVersion.kmsKeyId())
                .status(engineVersion.status())
                .tags(translateTagsFromSdk(engineVersion.tagList()))
                .build();
    }

    public static DescribeDbEngineVersionsRequest describeDbEngineVersionsRequest(final ResourceModel model,
                                                                                  final String nextToken) {
        return DescribeDbEngineVersionsRequest.builder()
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .marker(nextToken)
                .includeAll(true)
                .build();
    }

    public static List<ResourceModel> translateFromSdk(final Stream<DBEngineVersion> engineVersionsStream) {
        return engineVersionsStream
                .map(Translator::translateFromSdk)
                .collect(Collectors.toList());
    }

}
