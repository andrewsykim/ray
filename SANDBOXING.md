# Prototyping Ray Native Sandboxing

A growing industry trend involves using Ray for RL orchestration, with Ray actors managing API calls to the external sandbox services required for code execution or tool calls. While functional, this approach introduces significant complexity and friction, as developers need to embed sandbox-specific logic into their actor implementation. Furthermore, it fragments the ecosystem and reduces Ray's portability across environments that support different sandbox runtimes. This document outlines the scope of work to prototype a more native approach to sandboxing Ray actors that can integrate with popular existing secure runtimes like gVisor, firecracker VM, or agent substrate.

The goals of the prototype are to:
* Gauge the feasibility of native sandboxing in Ray
* Compare the pros / cons of native sandboxing, as opposed to calling external sandbox providers
* Evaluate the feasibility of using sandboxed Ray actors for agentic use-cases (agentic RL, long-running agents, etc)
* Create a Ray enhancement proposal based on the learnings (if PoC is successful)

## Prototype Scope

The proof of concept should enable the creation of Ray Actors whose remote methods are securely executed within a sandboxed runtime, such as gVisor. The actor must remain invokable via the standard Ray Actor API, however, the standard APIs can be extended if needed. Below is example code that should work with the proof of concept:

```python
import ray
import subprocess

@ray.remote
class SandboxedActor:
    def __init__(self, script, path):
        self.script = script
        self.path = path

    def execute_code(self):
        with open(self.path, "w") as f:
            f.write(self.script)

        command = ["python", self.path]
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            return {
                "status": "success",
                "stdout": result.stdout.strip(),
            }
        except subprocess.CalledProcessError as e:
            return {
                "status": "failed",
                "stderr": e.stderr.strip(),
            }

ray.init()

# dummy script
script = """import time
if __name__ == "__main__":
    time.sleep(1)
"""
path = "/home/ray/test.py"

# initialize the actor as a "sandboxed" actor
sandboxed_actor = SandboxedActor.options(
    sandbox_env=SandboxEnv(backend="docker", image_uri="python:3.11")
).remote(script, path)

# execute_code runs in the sandbox
result = ray.get(sandboxed_actor.execute_code.remote())
```

Requirements:
- All actor methods are executed in secure runtimes using containers.
- Sandboxed actors have guaranteed isolation.
- Code execution from one actor cannot impact another sandboxed actor or the Ray cluster.
- Processes running in the sandbox cannot initiate connections to other sandboxes or the originating Ray cluster.
- Multiple actors can run on the same “machine” without compromising the underlying host.

Stretch goals:
- Suspend / resume for sandboxed actors
- Warm pools for faster startup
- End-to-end example for agentic RL
- Prove feasibility with more than 1 secure runtime

## Implementation Details

To manage sandboxes and their states, we can leverage the existing runtime env agent architecture.
When Raylet starts up, it will start a "Sandbox Env Agent" process, much like Runtime Env Agent.
Raylet will make gRPC requests to this agent to create, start, and delete sandboxes.
The sandbox env agent will be responsible for maintaining the state of the sandbox environments and
can have multiple backend implementations for sandboxes. We will start with very standard approaches
like docker. Refer to the existing implementation for `RuntimeEnvAgent` and how it uses image_uri.
