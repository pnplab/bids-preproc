import itertools
from typing import Type
from . import TaskConfig, execute_strcmd, oneliner, dockerize, singularize, \
              logify, bashify
from ..path import OutputFile
from ..cli import VMEngine


class TaskFactory:
    # Returns a function that will execute the specified command with the
    # appropriate virtualisation setup (docker, singularity, direct, ...).
    # @warning Will capture all output (stdout & stderr), might overload RAM,
    # but only if logify is not used (is logFile variable is not set when user
    # executes the task).
    @classmethod
    def generate(cls: Type, vmType: VMEngine, taskConfig: TaskConfig):
        # Create task.
        def task(*args, **kargs):
            # Decorate command args (ie. path type placeholders, which can be used
            # by vm tool wrapper to setup the vm volumes, and performs path
            # checking and directory creation).
            newKargs = {}
            for arg, val in itertools.chain(enumerate(args), kargs.items()):
                # Convert arg to string (when needed), in order for *args to be
                # in the same format as **kargs, and thus be accessible from
                # dict. - for some reason this is required in order to avoid
                # `TypeError: keywords must be strings` exception when using
                # the task method with positional arguments.
                arg = str(arg)

                if arg in taskConfig.decorators:
                    wrapper = taskConfig.decorators[arg]
                    val = wrapper(val)
                newKargs[arg] = val

            # Inject command args within command template, and wrap with vm tool
            # when relevant.
            cmd = None
            if vmType == VMEngine.NONE:
                # @warning fmriprep anat fast track fix wont be injected if
                # docker or singularity is not used!
                cmd = str.format(
                    taskConfig.cmd,
                    taskConfig.raw_executable if '0' not in newKargs else newKargs['0'],
                    **newKargs
                )
            elif vmType == VMEngine.DOCKER:
                cmd = dockerize(
                    taskConfig.cmd,
                    taskConfig.docker_image if '0' not in newKargs else newKargs['0'],
                    **newKargs
                )
            elif vmType == VMEngine.SINGULARITY:
                cmd = singularize(
                    taskConfig.cmd,
                    taskConfig.singularity_image if '0' not in newKargs else newKargs['0'],
                    **newKargs
                )

            # Inject logs capture to the command.
            if 'logFile' in kargs:
                logFile = OutputFile(kargs['logFile'])  # Create dirs, etc.
                cmd = logify(cmd, logFile)

            # Merge the multiline command string as a single line, so the
            # command execution doesn't crash due to seperate, unwinllingly
            # splitted instructions.
            cmd = oneliner(cmd)

            # Ensure command is executed by bash shell (not sh).
            cmd = bashify(cmd)

            # Print command.
            print(f"cmd: {cmd}")

            # Execute command.
            taskResult = execute_strcmd(cmd)

            # Return command results.
            return taskResult

        return task
