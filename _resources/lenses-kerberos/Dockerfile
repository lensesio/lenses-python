FROM archlinux/base

# Update database
RUN pacman -Syu --noconfirm

# Install Kerberos, Python/Pip (In order to install dumb-init), Supervisor
RUN pacman -S vim inetutils  krb5 python python-pip supervisor --noconfirm

# Install dumb-init
RUN pip3 install dumb-init

# Clear Pacman cache
RUN pacman -Scc --noconfirm

# Add Kerberos configuration service
ADD ./config.sh /opt/config.sh

EXPOSE 88 749

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/opt/config.sh"]
