import lombok.*;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class ActionAdhocAccessRequest {
    private Long id;
    private String soeid; // approver/actor soeid
}