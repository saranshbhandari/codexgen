import lombok.*;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class RequestAdhocWorkflowAccessRequest {
    private String soeid;
    private Long wfid;
    private String description; // optional
}