package cl.barriodigital.requests.repository;

import cl.barriodigital.requests.entity.EstadoTramite;
import cl.barriodigital.requests.entity.Tramite;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface TramiteRepository extends JpaRepository<Tramite, String> {

    List<Tramite> findAllByOrderByFechaCreacionDesc();

    List<Tramite> findByPropietarioIdOrderByFechaCreacionDesc(String propietarioId);

    List<Tramite> findByEstado(EstadoTramite estado);

    long countByTipoIdAndFechaAdmisionBetween(String tipoId, LocalDateTime inicio, LocalDateTime fin);

    @Query("select t.tipoId, count(t) from Tramite t where t.fechaAdmision between :inicio and :fin group by t.tipoId")
    List<Object[]> countAdmittedByTipo(LocalDateTime inicio, LocalDateTime fin);

    long countByTipoId(String tipoId);
}
