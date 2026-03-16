FROM redhat/ubi9:latest

RUN yum install -y unzip less groff

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"

RUN unzip awscliv2.zip

RUN ./aws/install

RUN rm -rf awscliv2.zip aws

RUN yum clean all

ENTRYPOINT ["/bin/bash"]

