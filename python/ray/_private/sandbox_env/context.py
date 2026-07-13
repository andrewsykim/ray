import json


class SandboxEnvContext:
    def __init__(self, command_prefix=None):
        self.command_prefix = command_prefix or []

    def serialize(self):
        return json.dumps({"command_prefix": self.command_prefix})

    @classmethod
    def deserialize(cls, s):
        data = json.loads(s)
        return cls(command_prefix=data.get("command_prefix", []))
