Name:           @{package.name}
Version:        @{assembler.version}
Release:        @{assembler.revision}
Summary:        @{assembler.summary}
License:        @{assembler.licenses}
URL:            @{assembler.url}
Source0: assembler.tar
BuildArch:  noarch
BuildRequires: systemd
BuildRequires: java-1.8.0-openjdk-headless
BuildRequires: help2man
Requires: java-1.8.0-openjdk-headless
Requires: postgresql
Requires: mongodb
Requires: openssl

%description
@{assembler.description}

%prep
%setup -q -n assembler

%install
install -d %{buildroot}%{_sharedstatedir}/@{assembler.name}
install -d %{buildroot}%{_localstatedir}/log/@{assembler.name}
install -d %{buildroot}%{_localstatedir}/run/@{assembler.name}
install -d %{buildroot}%{_datadir}/@{assembler.name}
install -d %{buildroot}/%{_mandir}/man1/
install -p -D -m755 %{_builddir}/assembler/bin/@{assembler.name}.wrapper %{buildroot}%{_bindir}/@{assembler.name}
install -p -D -m755 %{_builddir}/assembler/bin/@{assembler.name}-setup.wrapper %{buildroot}%{_bindir}/@{assembler.name}-setup
install -p -D -m755 %{_builddir}/assembler/bin/@{assembler.name} %{buildroot}%{_datadir}/@{assembler.name}/bin/@{assembler.name}
install -p -D -m755 %{_builddir}/assembler/bin/@{assembler.name}-setup %{buildroot}%{_datadir}/@{assembler.name}/bin/@{assembler.name}-setup
install -p -D -m 644 %{_builddir}/assembler/systemd/@{assembler.name}.service %{buildroot}%{_unitdir}/@{assembler.name}.service
install -p -D -m 644 %{_builddir}/assembler/logrotate/@{assembler.name} %{buildroot}%{_sysconfdir}/logrotate.d/@{assembler.name}
install -p -D -m 644 %{_builddir}/assembler/conf/@{assembler.name}.yml %{buildroot}%{_sysconfdir}/@{assembler.name}/@{assembler.name}.yml
install -p -D -m 644 %{_builddir}/assembler/sysconfig/@{assembler.name}.sysconfig %{buildroot}%{_sysconfdir}/sysconfig/@{assembler.name}
install -p -D -m 644 %{_builddir}/assembler/doc/LICENSE-GNU_AGPLv3.txt %{buildroot}%{_datadir}/@{assembler.name}
install -p -D -m 644 %{_builddir}/assembler/doc/README.md %{buildroot}%{_datadir}/@{assembler.name}
cp -a %{_builddir}/assembler/lib %{buildroot}%{_datadir}/@{assembler.name}
help2man %{buildroot}%{_datadir}/@{assembler.name}/bin/@{assembler.name} -N > %{buildroot}/%{_mandir}/man1/@{assembler.name}.1

%files
%dir %attr(0755, torodb, root) %{_sharedstatedir}/@{assembler.name}
%dir %attr(0750, torodb, root) %{_localstatedir}/log/@{assembler.name}
%dir %attr(0755, torodb, root) %{_localstatedir}/run/@{assembler.name}
%{_bindir}/@{assembler.name}
%{_bindir}/@{assembler.name}-setup
%{_datadir}/@{assembler.name}/bin/@{assembler.name}
%{_datadir}/@{assembler.name}/bin/@{assembler.name}-setup
%{_datadir}/@{assembler.name}/lib/*
%config(noreplace) %{_sysconfdir}/logrotate.d/@{assembler.name}
%config(noreplace) %{_sysconfdir}/@{assembler.name}/@{assembler.name}.yml
%config(noreplace) %{_sysconfdir}/sysconfig/@{assembler.name}
%{_unitdir}/*.service
%license %{_datadir}/@{assembler.name}/LICENSE-GNU_AGPLv3.txt
%doc %{_datadir}/@{assembler.name}/README.md
%doc %{_mandir}/man1/@{assembler.name}.1*

%pre
getent group  torodb >/dev/null || groupadd -r torodb
getent passwd torodb >/dev/null || useradd -r -g torodb \
  -d %{_sharedstatedir}/@{assembler.name} -s /sbin/nologin \
  -c "@{assembler.fullName}" torodb

%post
%systemd_post @{assembler.name}.service

%preun
%systemd_preun @{assembler.name}.service

%postun
%systemd_postun_with_restart @{assembler.name}.service

%changelog
* @{rpm.date} @{assembler.maintainer} @{assembler.version}-@{assembler.revision}
- @{assembler.changelog}
