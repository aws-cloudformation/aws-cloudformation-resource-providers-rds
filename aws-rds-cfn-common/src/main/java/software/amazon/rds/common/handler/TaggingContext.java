package software.amazon.rds.common.handler;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode
public class TaggingContext {
    private boolean softFailTags;
    private boolean addTagsComplete;

    public interface Provider {
        TaggingContext getTaggingContext();
    }
}
