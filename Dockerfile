FROM golang:latest as BUILDER

MAINTAINER zengchen1024<chenzeng765@gmail.com>

# build binary
WORKDIR /go/src/github.com/opensourceways/robot-hook-dispatcher
COPY . .
RUN GO111MODULE=on CGO_ENABLED=0 go build -a -o robot-hook-dispatcher .

# copy binary config and utils
FROM alpine:3.14
COPY  --from=BUILDER /go/src/github.com/opensourceways/robot-hook-dispatcher/robot-hook-dispatcher /opt/app/robot-hook-dispatcher

ENTRYPOINT ["/opt/app/robot-hook-dispatcher"]
