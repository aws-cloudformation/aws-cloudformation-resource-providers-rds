package software.amazon.rds.dbinstance.request;

import software.amazon.cloudformation.proxy.ResourceHandlerRequest;

public class ValidatedRequest<T> extends ResourceHandlerRequest<T> {

    public ValidatedRequest(final ResourceHandlerRequest<T> base) {
        super(base.getClientRequestToken(),
                base.getDesiredResourceState(),
                base.getPreviousResourceState(),
                base.getDesiredResourceTags(),
                base.getPreviousResourceTags(),
                base.getSystemTags(),
                base.getPreviousSystemTags(),
                base.getAwsAccountId(),
                base.getAwsPartition(),
                base.getLogicalResourceIdentifier(),
                base.getNextToken(),
                base.getSnapshotRequested(),
                base.getRollback(),
                base.getDriftable(),
                base.getFeatures(),
                base.getUpdatePolicy(),
                base.getCreationPolicy(),
                base.getRegion(),
                base.getStackId());
    }
}
