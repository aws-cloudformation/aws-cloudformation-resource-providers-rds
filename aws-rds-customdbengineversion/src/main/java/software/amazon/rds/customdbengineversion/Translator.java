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
                .build();
    }

    static ModifyCustomDbEngineVersionRequest modifyCustomDbEngineVersionRequest(final ResourceModel model) {
        return ModifyCustomDbEngineVersionRequest.builder()
                .description(model.getDescription())
                .engine(model.getEngine())
                .engineVersion(model.getEngineVersion())
                .status(model.getStatus())
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

    static ResourceModel translateToModel(
            final DBEngineVersion engineVersion
    ) {
        return ResourceModel.builder()
                .databaseInstallationFilesS3BucketName(engineVersion.databaseInstallationFilesS3BucketName())
                .databaseInstallationFilesS3Prefix(engineVersion.databaseInstallationFilesS3Prefix())
                .description(engineVersion.dbEngineVersionDescription())
                .engine(engineVersion.engine())
                .engineVersion(engineVersion.engineVersion())
                .kMSKeyId(engineVersion.kmsKeyId())
                .tags(translateTagsFromSdk(engineVersion.tagList()))
                .dBEngineVersionArn(engineVersion.dbEngineVersionArn())
                .build();
    }

    public static DescribeDbEngineVersionsRequest describeDbEngineVersionsRequest(final String nextToken) {
        return DescribeDbEngineVersionsRequest.builder()
                .marker(nextToken)
                .build();
    }

    public static List<ResourceModel> translateToModel(final Stream<DBEngineVersion> engineVersionsStream) {
        return engineVersionsStream
                .map(Translator::translateToModel)
                .collect(Collectors.toList());
    }

}
