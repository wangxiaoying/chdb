# Dependencies

* llvm >= 16
* ccache

## python
* pybind11
* cannot use virtualenv (at least not success with pyenv due to the wrong `python-config --includes` result)
    * when using pyenv, can type `pyenv shell $version` to enter a shell with the non-virtualenv version to compile
