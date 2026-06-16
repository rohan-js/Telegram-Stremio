FROM docker.io/golang:1.26 AS build

ARG WIREPROXY_REF=7e755db

WORKDIR /usr/src
RUN git clone https://github.com/windtf/wireproxy.git wireproxy \
    && cd wireproxy \
    && git checkout "${WIREPROXY_REF}" \
    && make

FROM gcr.io/distroless/static-debian11:nonroot

COPY --from=build /usr/src/wireproxy/wireproxy /usr/bin/wireproxy

VOLUME ["/etc/wireproxy"]
ENTRYPOINT ["/usr/bin/wireproxy"]
CMD ["--config", "/etc/wireproxy/config"]
