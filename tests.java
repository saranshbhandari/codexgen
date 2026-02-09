package variable;

import java.util.*;

public class VariableStoreTestDriver {

    public static void main(String[] args) {
        runAllTests();
    }

    public static void runAllTests() {

        System.out.println("========== VariableStore Test ==========");

        // 1️⃣ Create store with default policy (REPLACE_WITH_EMPTY)
        VariableStore store = new VariableStore();
        store.setMissingVariablePolicy(
                VariableStore.MissingVariablePolicy.REPLACE_WITH_EMPTY
        );

        // 2️⃣ Add simple variables
        store.addVariable("SYSTEM", "Var1", "  hello world  ");
        store.addVariable("SYSTEM", "Number1", 10);
        store.addVariable("SYSTEM", "Number2", 5);

        // 3️⃣ Add collection variables
        store.addVariable("SYSTEM", "ARR", List.of("A", "B", "C"));

        Map<String, Object> user = new HashMap<>();
        user.put("name", "Saransh");
        user.put("city", "Chandigarh");

        store.addVariable("SYSTEM", "User", user);

        // 4️⃣ Basic resolution
        print(store, "Simple var",
                "${SYSTEM.Var1}");

        print(store, "Trim + Upper",
                "${FXN.UPPER(${FXN.TRIM(${SYSTEM.Var1})})}");

        // 5️⃣ Arithmetic
        print(store, "Add",
                "${FXN.ADD(${SYSTEM.Number1},${SYSTEM.Number2})}");

        print(store, "Max",
                "${FXN.MAX(${SYSTEM.Number1},${SYSTEM.Number2})}");

        // 6️⃣ Bracket access
        print(store, "Array index",
                "${SYSTEM.ARR[0]}");

        // 7️⃣ Map access
        print(store, "Map access",
                "${SYSTEM.User.name}");

        // 8️⃣ Null-safe navigation
        print(store, "Null-safe (existing)",
                "${SYSTEM.User?.city}");

        print(store, "Null-safe (missing)",
                "${SYSTEM.User?.address?.zip}");

        // 9️⃣ JOIN function
        print(store, "Join array",
                "${FXN.JOIN(${SYSTEM.ARR},\", \")}");

        // 🔟 Temporary variable test
        Map<String, Object> row = new HashMap<>();
        row.put("id", 101);
        row.put("status", "ACTIVE");

        try (var scope = store.withTempVariable("random.Row", row)) {

            print(store, "Temp variable access",
                    "RowId=${random.Row.id}, Status=${random.Row.status}");

            print(store, "Temp + IF",
                    "${FXN.IF(${random.Row.status},\"YES\",\"NO\")}");
        }

        // 1️⃣1️⃣ Missing variable policy tests
        store.setMissingVariablePolicy(
                VariableStore.MissingVariablePolicy.KEEP_AS_IS
        );
        print(store, "KEEP_AS_IS",
                "Value=${UNKNOWN.VAR}");

        store.setMissingVariablePolicy(
                VariableStore.MissingVariablePolicy.REPLACE_WITH_EMPTY
        );
        print(store, "REPLACE_WITH_EMPTY",
                "Value=${UNKNOWN.VAR}");

        store.setMissingVariablePolicy(
                VariableStore.MissingVariablePolicy.THROW_ERROR
        );
        try {
            print(store, "THROW_ERROR",
                    "Value=${UNKNOWN.VAR}");
        } catch (Exception e) {
            System.out.println("[EXPECTED ERROR] " + e.getMessage());
        }

        System.out.println("========== END ==========");
    }

    private static void print(VariableStore store, String label, String expr) {
        String result = store.resolveVariables(expr);
        System.out.println(String.format(
                "[%-25s] %s -> %s",
                label, expr, result
        ));
    }
}
