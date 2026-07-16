import json


class SandboxEnv:
    def __init__(self, backend="docker", image_uri=None):
        self.backend = backend
        self.image_uri = image_uri

    def to_dict(self):
        return {
            "backend": self.backend,
            "image_uri": self.image_uri,
        }

    def serialize(self):
        return json.dumps(self.to_dict())
