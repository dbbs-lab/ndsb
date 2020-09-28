import pathlib
import os
import pickle
import portalocker
import contextlib
import copy
import time
import uuid
import tarfile
import json
import shutil


__version__ = "0.1.0a0"


class Data:
    def __init__(self):
        pass

    def freeze(self, file="artifacts.pickle", timeout=10):
        with portalocker.Lock(file, "ab+", timeout=timeout) as fh:
            try:
                fh.seek(0, 2)
                if fh.tell():
                    fh.seek(0)
                    pickles = pickle.load(fh)
                else:
                    pickles = []
                pickles.append(self)
            except Exception as e:
                raise IOError(f"Corrupt artifact pickle file '{file}': {str(e)}")

            fh.seek(0)
            fh.truncate()
            pickle.dump(pickles, fh)

            fh.flush()
            os.fsync(fh.fileno())

    def pack(self, artifact):
        warnings.warn(f"The Data class {self.__class__.__name__} did not overwrite the `pack` method.")
        artifact.json({
            "not_implemented": True
        })
        with artifact.open("err.txt") as fh:
            fh.write(f"The Data class {self.__class__.__name__} did not overwrite the `pack` method.")

class ArtifactJson(dict):
    def __init__(self, instance):
        self._instance = instance

    def __call__(self, dict):
        self._instance._json.update(copy.deepcopy(dict))

class Artifact:

    def __init__(self, path):
        p = pathlib.Path(path)
        p.mkdir(exist_ok=False, parents=True)
        self.path = p
        self._json = {}
        self.json = ArtifactJson(self)

    def open(self, file, mode="x", timeout=10):
        return portalocker.Lock(str(self.path / file), mode, timeout=timeout)

    def finalize(self):
        with open(self.path / "artifact.json", "w") as f:
            json.dump(self.json, f)


class Beam:
    def __init__(self, archive):
        self.archive = archive

    def beam(self, host, key):
        pass


def thaw(file="artifacts.pickle", timeout=10):
    with portalocker.Lock(file, "rb+", timeout=timeout) as fh:
        pickles = pickle.load(fh)
        fh.seek(0)
        # Scramble the file before deleting it so that anything still trying to unpickle
        # it errors out, rather than silently creating a new file that will be
        # ignored.
        fh.write(b"all your frozen data melted, move along.")
    # After a brief delay, delete the scrambled file.
    time.sleep(0.1)
    os.remove(file)
    return pickles


def artificer(data, path=None, meta=None):
    id = str(uuid.uuid4())
    p = pathlib.Path(path or os.getcwd()) / id
    p.mkdir(exist_ok=False, parents=True)
    toplevel = p / "toplevel.json"
    with open(toplevel, "w") as f:
        json.dump(meta or dict(), f)
    for i, datum in enumerate(data):
        artifact = Artifact(p / str(i))
        datum.pack(artifact)
        artifact.finalize()
    with tarfile.open(p.parents[0] / (id + ".tar.gz"), "w:gz") as tar:
        tar.add(p, arcname=id)
    shutil.rmtree(p)
    return Beam(str(p) + ".tar.gz")
