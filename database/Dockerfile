FROM postgres:15

# Copy dump and post-restore SQL into the image
COPY de_test_task_db /tmp/de_test_task_db
COPY 01_pipeline_setup.sql /docker-entrypoint-initdb.d/02_pipeline_setup.sql

# Create the restore script *inside* the image so it always has LF line
# endings regardless of the host OS (avoids Windows CRLF breakage).
RUN printf '#!/bin/bash\nset -e\necho "[restore] Starting pg_restore..."\npg_restore --no-owner --no-privileges -U "$POSTGRES_USER" -d "$POSTGRES_DB" /tmp/de_test_task_db\necho "[restore] Done."\n' \
        > /docker-entrypoint-initdb.d/01_restore.sh \
    && chmod +x /docker-entrypoint-initdb.d/01_restore.sh
