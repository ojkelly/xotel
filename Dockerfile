FROM alpine:3.15.0

COPY xotel /srv/xotel

WORKDIR "/xotel"

ENTRYPOINT [ "/srv/xotel" ]