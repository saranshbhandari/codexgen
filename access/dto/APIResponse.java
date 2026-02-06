import lombok.*;

@Getter @Setter
@AllArgsConstructor @NoArgsConstructor
@Builder
public class APIResponse<T> {
    private boolean success;
    private String message;
    private T data;

    public static <T> APIResponse<T> ok(String msg, T data) {
        return APIResponse.<T>builder().success(true).message(msg).data(data).build();
    }

    public static <T> APIResponse<T> fail(String msg) {
        return APIResponse.<T>builder().success(false).message(msg).data(null).build();
    }
}