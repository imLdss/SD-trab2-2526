FROM nunopreguica/sd2526tpbase

# working directory inside docker image
WORKDIR /home/sd

ADD hibernate.cfg.xml .
ADD messages.props .
COPY tls/ .

COPY target/sd2526-tp1-ref-1.jar sd2526.jar
