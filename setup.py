#!/usr/bin/env python
# Always prefer setuptools over distutils
import os, imp
from setuptools import setup, find_packages

__doc__ = """

To install as system package:

  python setup.py install
  
To install as local package, just run:

  mkdir /tmp/builds/
  python setup.py install --root=/tmp/builds
  /tmp/builds/usr/bin/$DS -? -v4

To tune some options:

  RU=/opt/control
  python setup.py egg_info --egg-base=tmp install --root=$RU/files --no-compile \
    --install-lib=lib/python/site-packages --install-scripts=ds

-------------------------------------------------------------------------------
"""

print(__doc__)

version = open('PyTangoArchiving/VERSION').read().strip()
scripts = []
license = 'GPL-3.0'

f = './PyTangoArchiving/scripts/'
scripts = [
f+'taurusfinder',
f+'ctarchiving',
f+'ctsnaps',
f+'archiving2csv',
f+'archiving2plot',
#f+'archiving_report.py',
f+'archiving_service',
#f+'archiver_health_check.py',
#f+'cleanTdbFiles',
#f+'db_repair.py',
]

entry_points = {
        'console_scripts': [
            #'CopyCatDS = PyTangoArchiving.interface.CopyCatDS:main',
        ],
}

# EXTRA FILES ARE ADDED IN MANIFEST.in, not here

#package_data = {
#    'PyTangoArchiving': [#'VERSION','README',
#         './VERSION',
         #'./CHANGES',
#         './widget/ui/TaurusAttributeChooser.ui',
         #'./widget/resources/*',
         #'./widget/resources.qrc',
         #'./widget/snaps/ui/*',
         #'./widget/snaps/doc/snapimg/*png',
         #'./widget/snaps/README',
#         ],
#}

setup(
    name="PyTangoArchiving",
    version=str(version),
    license=license,
    packages=find_packages(),
    description="Python bindings for Tango Control System Archiving",
    long_description="This package allows to: \n"
    "* Integrate Hdb and Snap archiving with other python/PyTango tools.\n"
    "* Start/Stop Archiving devices in the appropiated order.\n"
    "* Increase the capabilities of configuration and diagnostic.\n"
    "* Import/Export .csv and .xml files between the archiving and the database.",
    author="Sergi Rubio",
    author_email="srubio@cells.es",
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Other Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: '\
            'GNU Lesser General Public License v3 or later (LGPLv3+)',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: Software Development :: Libraries',
    ],
    platforms=[ "Linux" ],
    scripts=scripts,
    entry_points=entry_points,
    include_package_data=True,
    #package_data=package_data,
    install_requires=['fandango','PyTango','MySQL-python'],
    zip_safe=False
  )
