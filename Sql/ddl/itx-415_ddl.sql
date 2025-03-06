CREATE TABLE control.uploaded_files_itx (
    id TEXT,
    Periodo integer,
    client varchar(30),
    file_name varchar(150),
    status_code VARCHAR(10),
    detail_type VARCHAR(50),
    protocol VARCHAR(10),
    bytes INTEGER,
    file_path TEXT,
    username VARCHAR(30),
    session_id TEXT,
    date_send TIMESTAMP,
    failure_message TEXT,
    t_fch_load TIMESTAMP,
    PRIMARY KEY (id, Periodo)
);
CREATE INDEX idx_client ON control.uploaded_files_itx (client);