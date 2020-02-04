#
# spec file for package vthydra
#
# Copyright (c) 2020 SUSE LLC
#
# All modifications and additions to the file contributed by third parties
# remain the property of their copyright owners, unless otherwise agreed
# upon. The license for this file, and modifications and additions to the
# file, is the same license as for the pristine package itself (unless the
# license for the pristine package is not an Open Source License, in which
# case the license is the MIT License). An "Open Source License" is a
# license that conforms to the Open Source Definition (Version 1.9)
# published by the Open Source Initiative.

# Please submit bugfixes or comments via https://bugs.opensuse.org/
#


Name:           vthydra
Version:        0
Release:        0
Summary:        HMC VT multiplexer
License:        MIT
Source:         vthydra.rb
Source1:        vthydra.conf
# expect is fake dependency for the script that uses this package
Requires:       expect
Requires:       ruby
Requires:       ruby-poll
BuildArch:      noarch

%description
HMC VT multiplexer

%prep
cp -a %{SOURCE2} %{SOURCE1} %{SOURCE0} . ||:

%build

%install
install -m 755 -D -t %{buildroot}%{_bindir} vthydra.rb
install -m 644 -D -t %{buildroot}%{_sysconfdir} vthydra.conf
for i in dspmsg mkvterm rmvterm lshmc lssyscfg lscomgmt chsysstate; do
ln -s  vthydra.rb %{buildroot}%{_bindir}/$i
done
mkdir -p %{buildroot}/opt/hsc
ln -s %{_bindir} %{buildroot}/opt/hsc/bin

%post

%postun

%files
%doc vthydra.conf
%{_bindir}/*
/opt/hsc
%config %{_sysconfdir}/vthydra.conf

%changelog
