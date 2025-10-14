FROM 653528873951.dkr.ecr.us-west-2.amazonaws.com/docker-hub/library/python:3.13-slim-trixie AS builder


RUN apt-get update
RUN apt-get install -y --no-install-recommends curl ca-certificates binutils

ADD https://astral.sh/uv/install.sh /uv-installer.sh

RUN sh /uv-installer.sh && rm /uv-installer.sh

ENV PATH="/root/.local/bin/:$PATH"

RUN mkdir /app
COPY . /app
WORKDIR /app
RUN uv sync --frozen
RUN uv run --frozen python -m PyInstaller src/load_everything.py -n leaguepedia-loader --onefile

FROM 653528873951.dkr.ecr.us-west-2.amazonaws.com/docker-hub/library/python:3.13-slim-trixie AS runner

RUN apt-get update
RUN apt-get install -y binutils

COPY --from=builder /app/dist/leaguepedia-loader /usr/local/bin/

CMD ["/bin/bash"]
