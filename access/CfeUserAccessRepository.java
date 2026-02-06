import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.*;

public interface CfeUserAccessRepository extends JpaRepository<CfeUserAccess, Long> {

    List<CfeUserAccess> findBySoeidAndIsActive(String soeid, String isActive);

    @Query("""
        select ua.pid
        from CfeUserAccess ua
        join ua.role r
        where ua.soeid = :soeid
          and ua.isActive = 'Y'
          and r.hasSpecificWriteAccessApproval = 'Y'
    """)
    List<String> findProjectIdsWhereUserCanApprove(@Param("soeid") String soeid);
}