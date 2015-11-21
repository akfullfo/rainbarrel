#!/usr/bin/env python

import re, sys, os
from setuptools import setup, find_packages

name = 'rainbarrel'
version_file = os.path.join(name, '__init__.py')
readme_file = 'README.md'
version = ''
with open(version_file, 'rt') as f:
	m = re.search(r'''__version__\s*=\s*["'](\d+(\.\d+)+)["']''', f.read())
	if m:
		version = m.group(1)
if not re.match(r'^\d+(\.\d+)+$', version):
	raise Exception("Invalid version '%s' found in version file '%s'" % (version, version_file))

requires = ['PyYAML>=3.09', 'netaddr>=0.7.11', 'python-dateutil>=2.4.2', 'taskforce>=0.3.14']

setup_parms = {
	'name': name,
	'provides': [name],
	'version': version,
	'description': """Rainbarrel receives webhook POSTs from a Rainbow Eagle smart meter monitoring unit.
The data ireceived is provided to plugins or external programs for data logging and control.""",
	'author': "Andrew Fullford",
	'author_email': "git042013@fullford.com",
	'maintainer': "Andrew Fullford",
	'maintainer_email': "pypi102014@fullford.com",
	'url': "https://github.com/akfullfo/rainbarrel",
	'download_url': "https://github.com/akfullfo/rainbarrel/tarball/" + version,
	'license': "Apache License, Version 2.0",
	'include_package_data': True,
	'platforms': ['Linux', 'BSD', 'Mac OS X'],
	'classifiers': [
		'Development Status :: 3 - Alpha',
		'Environment :: No Input/Output (Daemon)',
		'Intended Audience :: Developers',
		'License :: OSI Approved :: Apache Software License',
		'Operating System :: POSIX :: Linux',
		'Operating System :: POSIX :: BSD',
		'Operating System :: MacOS :: MacOS X',
		'Programming Language :: Python :: 2.7',
		#'Programming Language :: Python :: 3.3',
		#'Programming Language :: Python :: 3.4',
		'Topic :: Home Automation',
		'Topic :: System :: Hardware',
		'Topic :: System :: Monitoring',
		'Topic :: Utilities'
	],

	'packages': [name, name + '.plugins'],
	'scripts': [os.path.join('bin', name)],
	'requires': [re.sub(r'\W.*', '', item) for item in requires],
	'install_requires': requires
}
try:
	with open(readme_file, 'rt') as f:
		setup_parms['long_description'] = f.read()
except:
	pass

setup(**setup_parms)
