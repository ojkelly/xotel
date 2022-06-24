FROM alpine:3.15.0
# copy over the binary from the first stage
COPY xotel /xotel/xotel
WORKDIR "/xotel"
ENTRYPOINT [ "/xotel/xotel" ]
