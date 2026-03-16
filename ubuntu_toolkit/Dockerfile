FROM redhat/ubi9:latest

RUN dnf install -y \
      unzip \
      less \
      groff \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws \
    && dnf clean all

ENTRYPOINT ["/bin/bash"]
