package cl.barriodigital.common;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public record TramiteEvent(UUID eventId, UUID tramiteId, String eventType, Instant occurredAt,
                          String estadoAnterior, String estado, String actor, String correlationId) {
    public static final String NEGOCIO = "tramites-eventos";
    public static final String AUDITORIA = "tramites-auditoria";
    public static final String NEGOCIO_DLT = NEGOCIO + "-dlt";
    public static final String AUDITORIA_DLT = AUDITORIA + "-dlt";
    private static final Set<String> ESTADOS = Set.of("INGRESADO","ADMITIDO","EN_GESTION","EN_TERRENO","RESUELTO","RECHAZADO");

    public void validate(String key) {
        if (eventId == null || tramiteId == null || !tramiteId.toString().equals(key)
                || occurredAt == null || actor == null || actor.isBlank() || actor.length() > 255
                || correlationId == null || correlationId.isBlank() || correlationId.length() > 100
                || estado == null || !ESTADOS.contains(estado))
            throw new IllegalArgumentException("Datos de trámite incompletos");
        boolean ingreso = "TRAMITE_INGRESADO".equals(eventType) && "INGRESADO".equals(estado) && estadoAnterior == null;
        boolean cambio = "ETAPA_CAMBIADA".equals(eventType) && estadoAnterior != null && ESTADOS.contains(estadoAnterior) && !estadoAnterior.equals(estado);
        if (!ingreso && !cambio) throw new IllegalArgumentException("Tipo o estado inválido");
    }
}
