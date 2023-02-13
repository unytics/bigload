
    @property
    def virtualenv_folder(self):
        return f'{self.base_virtualenv_folder}/{self.name}'

    @property
    def python_folder(self):
        folder = {'Linux': 'bin', 'Darwin': 'bin', 'Windows': 'Scripts'}[platform.system()]
        return f'{self.virtualenv_folder}/{folder}'

    @property
    def python_exe(self):
        return str(pathlib.Path(f'{self.python_folder}/python'))

    def install(self):
        if os.path.exists(self.virtualenv_folder):
            shutil.rmtree(self.virtualenv_folder)
        venv.create(self.virtualenv_folder, with_pip=True)
        requirements = [f'"{requirement}"' for requirement in self.python_requirements]
        os.system(f'{self.python_exe} -m pip install {" ".join(requirements)}')

    def activate_virtual_env(self):
        python_folder = os.path.abspath(f'{self.python_folder}/activate_this.py')  # Looted from virtualenv; should not require modification, since it's defined relatively
        base_folder = os.path.dirname(python_folder)
        print(python_folder)
        print(base_folder)

        # prepend bin to PATH (this file is inside the bin directory)
        os.environ["PATH"] = os.pathsep.join([self.python_folder] + os.environ.get("PATH", "").split(os.pathsep))
        os.environ["VIRTUAL_ENV"] = base_folder  # virtual env is right above bin directory

        # add the virtual environments libraries to the host python import mechanism
        prev_length = len(sys.path)
        libs_folder = ['Lib/site-packages'] if platform.system() == 'Windows' else ['foo']
        for lib in libs_folder:
            print(lib)
            path = os.path.realpath(os.path.join(base_folder, lib))
            print(path)
            site.addsitedir(path)
        print(site.getsitepackages())

        sys.path[:] = sys.path[prev_length:] + sys.path[0:prev_length]
        sys.real_prefix = sys.prefix
        sys.prefix = base_folder
        print(site.getsitepackages())
