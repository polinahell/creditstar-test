FROM postgres:15

COPY de_test_task_db /tmp/de_test_task_db
COPY 01_pipeline_setup.sql /docker-entrypoint-initdb.d/02_pipeline_setup.sql

# Generate the restore script inside the image (guarantees LF line endings).
# pg_restore may emit warnings on PG version mismatch – treat them as non-fatal.
RUN printf '#!/bin/bash\necho "[restore] Starting pg_restore..."\npg_restore --no-owner --no-privileges -U "$POSTGRES_USER" -d "$POSTGRES_DB" /tmp/de_test_task_db || echo "[restore] Finished with warnings (non-fatal)."\necho "[restore] Done."\n' \
        > /docker-entrypoint-initdb.d/01_restore.sh \
    && chmod +x /docker-entrypoint-initdb.d/01_restore.sh
