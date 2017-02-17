Name:           @{assembler.name}
Version:        @{project.version}
Release:        @{assembler.revision}
Summary:        @{project.name}
License:        @{assembler.licenses}
URL:            @{assembler.url}
Source0: assembler.tar
BuildArch:  noarch
BuildRequires: java-1.8.0-openjdk-headless
BuildRequires: help2man
Requires: java-1.8.0-openjdk-headless
Requires: postgresql

%description
@{project.description}

%prep
%setup -q -n assembler

%install
install -d %{buildroot}%{_sharedstatedir}/%{name}
install -d %{buildroot}%{_localstatedir}/log/%{name}
install -d %{buildroot}%{_localstatedir}/run/%{name}
install -d %{buildroot}%{_datadir}/%{name}
install -d %{buildroot}/%{_mandir}/man1/
install -p -D -m755 %{_builddir}/assembler/bin/%{name}.wrapper %{buildroot}%{_bindir}/%{name}
install -p -D -m755 %{_builddir}/assembler/bin/%{name}-setup.wrapper %{buildroot}%{_bindir}/%{name}-setup
install -p -D -m755 %{_builddir}/assembler/bin/%{name} %{buildroot}%{_datadir}/%{name}/bin/%{name}
install -p -D -m755 %{_builddir}/assembler/bin/%{name}-setup %{buildroot}%{_datadir}/%{name}/bin/%{name}-setup
install -p -D -m 644 %{_builddir}/assembler/systemd/%{name}.service %{buildroot}%{_unitdir}/%{name}.service
install -p -D -m 644 %{_builddir}/assembler/logrotate/%{name} %{buildroot}%{_sysconfdir}/logrotate.d/%{name}
install -p -D -m 644 %{_builddir}/assembler/conf/%{name}.yml %{buildroot}%{_sysconfdir}/%{name}/%{name}.yml
install -p -D -m 644 %{_builddir}/assembler/sysconfig/%{name}.sysconfig %{buildroot}%{_sysconfdir}/sysconfig/%{name}
install -p -D -m 644 %{_builddir}/assembler/doc/LICENSE-GNU_AGPLv3.txt %{buildroot}%{_datadir}/%{name}
install -p -D -m 644 %{_builddir}/assembler/doc/README.md %{buildroot}%{_datadir}/%{name}
cp -a %{_builddir}/assembler/lib %{buildroot}%{_datadir}/%{name}
help2man %{buildroot}%{_datadir}/%{name}/bin/%{name} -N > %{buildroot}/%{_mandir}/man1/%{name}.1

%files
%dir %attr(0755, torodb, root) %{_sharedstatedir}/%{name}
%dir %attr(0750, torodb, root) %{_localstatedir}/log/%{name}
%dir %attr(0755, torodb, root) %{_localstatedir}/run/%{name}
%{_bindir}/%{name}
%{_bindir}/%{name}-setup
%{_datadir}/%{name}/bin/%{name}
%{_datadir}/%{name}/bin/%{name}-setup
%{_datadir}/%{name}/lib/*
%config(noreplace) %{_sysconfdir}/logrotate.d/%{name}
%config(noreplace) %{_sysconfdir}/%{name}/%{name}.yml
%config(noreplace) %{_sysconfdir}/sysconfig/%{name}
%{_unitdir}/*.service
%license %{_datadir}/%{name}/LICENSE-GNU_AGPLv3.txt
%doc %{_datadir}/%{name}/README.md
%doc %{_mandir}/man1/%{name}.1*

%pre
getent group  torodb >/dev/null || groupadd -r torodb
getent passwd torodb >/dev/null || useradd -r -g torodb -u 184 \
  -d %{_sharedstatedir}/%{name} -s /sbin/nologin \
  -c "@{assembler.fullName}" torodb

%post
%systemd_post %{name}.service

%preun
%systemd_preun %{name}.service

%postun
%systemd_postun_with_restart %{name}.service

%changelog
@{assembler.changelog}
