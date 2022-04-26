package software.amazon.rds.common.util;

import lombok.NonNull;
import software.amazon.awssdk.utils.StringUtils;
import software.amazon.cloudformation.resource.IdentifierUtils;

public class IdentifierFactory {

    private final String defaultStackId;
    private final String defaultResourceId;
    private final int maxLength;

    public IdentifierFactory(
            @NonNull final String defaultStackId,
            @NonNull final String defaultResourceId,
            final int maxLength
    ) {
        this.defaultStackId = defaultStackId;
        this.defaultResourceId = defaultResourceId;
        this.maxLength = maxLength;
    }

    public Identifier newIdentifier() {
        return new Identifier(this);
    }

    public static class Identifier {

        private final IdentifierFactory identifierFactory;
        private String stackId;
        private String resourceId;
        private String requestToken;

        private Identifier(IdentifierFactory identifierFactory) {
            this.identifierFactory = identifierFactory;
        }

        public Identifier withStackId(final String stackId) {
            this.stackId = stackId;
            return this;
        }

        public Identifier withResourceId(final String resourceId) {
            this.resourceId = resourceId;
            return this;
        }

        public Identifier withRequestToken(final String requestToken) {
            this.requestToken = requestToken;
            return this;
        }

        public String toString() {
            final String identifier = IdentifierUtils.generateResourceIdentifier(
                    StringUtils.isEmpty(stackId) ? identifierFactory.defaultStackId : stackId,
                    StringUtils.isEmpty(resourceId) ? identifierFactory.defaultResourceId : resourceId,
                    StringUtils.isEmpty(requestToken) ? "" : requestToken,
                    identifierFactory.maxLength
            );
            return identifier
                    .replaceFirst("^-+", "")
                    .replaceAll("-{2,}", "-")
                    .replaceAll("-+$", "")
                    .toLowerCase();
        }
    }
}
