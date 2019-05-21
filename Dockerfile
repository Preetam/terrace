FROM alpine:3.8

ADD build/terrace /bin/terrace

ENTRYPOINT ["/bin/terrace"]
CMD []
