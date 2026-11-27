"""Build against the same Arrow ABI supplied by the runtime PyArrow wheel."""
import glob
import os
import platform
import sys
from pathlib import Path

import pyarrow
from pybind11.setup_helpers import Pybind11Extension, build_ext
from setuptools import setup

if sys.platform == 'darwin':
    # Apple's Python may default to universal2, but Arrow wheels are single-arch.
    os.environ.setdefault('ARCHFLAGS', '-arch ' + platform.machine())
    os.environ.setdefault('MACOSX_DEPLOYMENT_TARGET', '12.0')

arrow_dir = pyarrow.get_library_dirs()[0]
def library(name):
    suffix = 'dylib' if sys.platform == 'darwin' else 'so*'
    matches = glob.glob(str(Path(arrow_dir) / ('lib' + name + '.*' + suffix)))
    if sys.platform != 'darwin':
        matches += glob.glob(str(Path(arrow_dir) / ('lib' + name + '.so*')))
    if not matches:
        raise RuntimeError('Missing Arrow library: ' + name)
    return min(matches, key=len)

relative = '@loader_path/../pyarrow' if sys.platform == 'darwin' else '$ORIGIN/../pyarrow'
wheel_options = ({'bdist_wheel': {'plat_name': 'macosx_12_0_' + platform.machine()}}
                 if sys.platform == 'darwin' else {})
setup(options=wheel_options, ext_modules=[Pybind11Extension(
    'certskip._native',
    sorted(glob.glob('cpp/*.cpp')),
    include_dirs=[pyarrow.get_include()],
    extra_objects=[library(name) for name in ('arrow', 'arrow_python', 'parquet')],
    extra_link_args=['-Wl,-rpath,' + relative, '-Wl,-rpath,' + arrow_dir],
    cxx_std=20,
)], cmdclass={'build_ext': build_ext})
