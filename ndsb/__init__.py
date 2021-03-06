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
import datetime

__version__ = "2.0.0"

class BeamError(Exception):
    pass

class IntruderAlert(Exception):
    pass

class RestrictAccess:
    def __init__(self):
        self._private = False
        self._whitelist = set()

    def make_private(self):
        self._private = True

    def grant_access(self, whitelist):
        if not self._private:
            raise RuntimeError("Cannot grant access on public object, make private first.")
        self._whitelist.update(whitelist)

class Data:
    def __init__(self):
        self.artifact = None

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

    def view(self):
        return self.to_markdown("*Override your `Data` class' `view` method for a better description.*")

    def to_markdown(self, content=""):
        id_str = f"<sub>id:{self.artifact.id}</sub>" if self.artifact else ""
        return (
            f"\n## {self.__class__.__name__} artifact{id_str}\n\n"
            + content
            + ("\n\n" if bool(self.artifact) else "")
            + (f"\n```json\n{json.dumps(self.artifact.json, indent=2)}\n```" if bool(self.artifact) else "")
        )



class ArtifactJson(dict):
    def __call__(self, dict):
        self.update(copy.deepcopy(dict))


class Artifact(RestrictAccess):
    def __init__(self, path):
        super().__init__()
        p = pathlib.Path(path)
        p.mkdir(exist_ok=False, parents=True)
        self.path = p
        self._json = {}
        self.json = ArtifactJson()

    @property
    def id(self):
        return self.path.parts[-1]

    def open(self, file, mode="x", timeout=10):
        return portalocker.Lock(str(self.path / file), mode, timeout=timeout)

    def finalize(self):
        self.json["public_access"] = not self._private
        if self._private and self._whitelist:
            self.json["access_list"] = list(self.whitelist)
        with open(self.path / "artifact.json", "w") as f:
            json.dump(self.json, f)


class Beam(RestrictAccess):
    def __init__(self, data, archive):
        super().__init__()
        self.archive = archive
        self.data = data

    def fire(self, host, launch_codes, debug=False, fire_at_strangers=False):
        import requests

        endpoint = os.path.join(host, "beam", "receive") + "/"
        try:
            response = requests.post(endpoint, **self.initiate_firing_protocol(launch_codes), verify=not fire_at_strangers)
        except requests.exceptions.SSLError as e:
            if "CERTIFICATE_VERIFY_FAILED" in str(e):
                raise IntruderAlert("SSL Certificate could not be verified. If you know this is OK, use `beam.fire(..., fire_at_strangers=True)`")

        if response.status_code == 200:
            if not debug:
                os.remove(self.archive)
        else:
            raise BeamError(f"Could not beam '{self.archive}': Error [{response.status_code}] {response.text}")

        try:
            json_response = json.loads(response.text)
        except:
            raise BeamError(
                "\n-----------------------Response--------------------------\n"
                + response.text
                + "\n---------------------------------------------------------"
                + f"\n\nInvalid {response.status_code} response to '{self.archive}' (see decoding error follow by raw response above)"
            )

        endpoint = os.path.join(os.path.dirname(host[:-1]), endpoint, "emit")
        beam_id = json_response["id"]
        for i, datum in enumerate(self.data):
            datum.artifact.remote_path = os.path.join(endpoint, beam_id, str(i))

        return json_response

    def initiate_firing_protocol(self, key):
        from requests_toolbelt.multipart.encoder import MultipartEncoder
        mp_encoder = MultipartEncoder(
            fields={
                'meta': json.dumps(self.tune_frequencies()),
                'archive': ("archive.tar.gz", self.charge_beam(), "application/gzip"),
            }
        )

        return {
            "headers": {
                "Authorization": f"Bearer {key}",
                "Content-Type": mp_encoder.content_type
            },
            "data": mp_encoder
        }

    def tune_frequencies(self):
        """
            Get the metadata
        """
        tuning_data = {
            "transmitted_on": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "transmitted_by": os.getenv("NDSB_TRANSMITTER_NAME", "Unknown"),
            "public_access": not self._private,
        }
        if self._private and self._whitelist:
            tuning_data["access_list"] = list(self.whitelist)
        return tuning_data

    def charge_beam(self):
        return open(self.archive, "rb")


def aim(host, client_id, client_secret, username=None, password=None, aim_at_strangers=False):
    import requests

    data = {"client_id": client_id, "client_secret": client_secret}
    if username:
        data["username"] = username
        data["password"] = password
        data["grant_type"] = "password"
    else:
        data["grant_type"] = "client"

    endpoint = os.path.join(host, "o", "token") + "/"
    response = requests.post(endpoint, data=data, verify=not aim_at_strangers)
    try:
        return json.loads(response.text)["access_token"]
    except:
        raise BeamError(
            "\n-----------------------Response--------------------------\n"
            + response.text
            + "\n---------------------------------------------------------"
            + f"\n\nInvalid {response.status_code} authentication response (see decoding error follow by raw response above)"
        )


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
        datum.artifact = artifact
    with tarfile.open(p.parents[0] / (id + ".tar.gz"), "w:gz") as tar:
        tar.add(p, arcname=id)
    shutil.rmtree(p)
    return Beam(data, str(p) + ".tar.gz")
