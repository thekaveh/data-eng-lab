FROM --platform=linux/amd64 python@sha256:cec9aa7aa96eea4fa036e9b82be1e6b325f2e3707f462d885868df51ec0a4b47
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8 TZ=UTC PYTHONHASHSEED=0
COPY datasets/tpch-lock-requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir --require-hashes -r /tmp/requirements.txt
COPY uv.lock /workspace/uv.lock
RUN echo "a376ce1b5bd5621290aaded68c22572690395419876da41814e28469bb4186b1  /workspace/uv.lock" | sha256sum -c -
RUN python -c "import duckdb; duckdb.connect().execute('INSTALL tpch')" && \
    echo "a6516e487106b4f95bd6d85da4364debdcb2db536d015784bc43209af6ed0125  /root/.duckdb/extensions/v1.5.4/linux_amd64/tpch.duckdb_extension" | sha256sum -c -
WORKDIR /workspace
COPY datasets /workspace/datasets
ENTRYPOINT ["python", "-m", "datasets.tpch_lock_export"]
