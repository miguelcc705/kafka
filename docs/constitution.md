# Constitución del proyecto

Principios que no se negocian durante el desarrollo. Se derivan de [SPEC.md](../SPEC.md). Si algo de aquí choca con una idea nueva, gana este documento hasta que se acuerde un cambio explícito.

## 1. La arquitectura ya está aprobada

- Las decisiones marcadas como **[Decidido]** en el SPEC no se cambian sin preguntar:
  - Solo se acepta la moneda `COP`.
  - La clave de `transfer.created` es `fromAccount`.
  - Las migraciones se hacen con Flyway, en SQL plano.
  - transfer-service publica sus eventos mediante el patrón outbox, con un publicador `@Scheduled` y sin Debezium.
  - La arquitectura interna es por capas: `api`, `service`, `domain`, `repository`, `messaging`, `config` (y `rules` en fraud-service).
- Si algo no está definido en el SPEC, **se pregunta antes de asumir**. Si se asume algo, se dice explícitamente al cerrar la fase.

## 2. Simplicidad sobre sobreingeniería

- No se agregan librerías ni servicios que no estén en el SPEC: nada de API Gateway, service discovery, Kubernetes, Lombok ni Debezium.
- Se prefiere la solución simple y legible.
- Los comentarios explican el **porqué** de una decisión que no es obvia, no el qué.

## 3. Cada servicio es dueño de sus datos y de su contrato

- Cada servicio tiene su propia base PostgreSQL. Ningún servicio lee la base del otro.
- No hay librería compartida. El contrato entre servicios es el **JSON del evento**: cada servicio define sus propios `record`, y los type headers de Kafka están desactivados.

## 4. Mensajería confiable

- La semántica es **al menos una vez**. Todo consumidor tolera mensajes duplicados (idempotencia).
- En los productores: `acks=all` y `enable.idempotence=true`.
- En los consumidores: commit manual por registro (`AckMode.RECORD`) y `auto.offset.reset=earliest`.
- Los errores se reintentan 3 veces con backoff (1 s, 2 s, 4 s) y después van a la DLT. Los errores de deserialización van directo a la DLT.
- El `correlationId` viaja de punta a punta: header HTTP → MDC → outbox → header de Kafka → MDC.

## 5. Nada se da por hecho sin ejecutarlo

- Cada fase tiene un comando de verificación (ver [tasks.md](../specs/001-transfer-fraud/tasks.md)). La fase no termina hasta ejecutarlo y verlo pasar.
- No se afirma que algo funciona sin haberlo ejecutado.
- Las versiones de las dependencias e imágenes se verifican antes de fijarlas, y nunca se usa `latest`.

## 6. Forma de trabajo

- Se trabaja por fases. **Al terminar cada fase el trabajo se detiene** hasta que el dueño del proyecto la revise y dé la indicación de seguir.
- Se escriben archivos completos, sin fragmentos `...` ni pseudocódigo.
- Se hace un commit por fase (o por bloque lógico) con Conventional Commits, con el servicio como alcance cuando aplique, por ejemplo `feat(fraud-service): add velocity rule`.
- Cada fase cierra con un resumen breve de lo que se hizo, las decisiones tomadas y los supuestos nuevos.
