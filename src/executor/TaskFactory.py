import itertools
from typing import Dict
from . import execute_strcmd, dockerize, singularize, oneliner
from ..path import PathPlaceHolder, OutputFile
from ..cli import VMEngine


# @warning set -o pipefail is only compatible with bash, not
# dash (debian).
def logify(cmd, logPath):
    return f'''(set -o pipefail && {cmd} 2>&1 | tee "{logPath}")'''


# @warning output cannot be merged into a single.
def bashify(cmd):
    # we need heredoc syntax in order to allow inner cmd quotes without
    # extra escaping.
    return ('bash <<__BASH_CMD_END__' '\n'
            f'{cmd}' '\n'
            '__BASH_CMD_END__')


class TaskFactory:
    # Returns a function that will execute the specified command with the
    # appropriate virtualisation setup (docker, singularity, direct, ...).
    @classmethod
    def generate(vmType: VMEngine, executable: str, cmdTemplate: str,
                 **argsDecorators: Dict[str, PathPlaceHolder]):
        # Create task.
        def task(*args, **kargs):
            # Decorate command args (ie. path type placeholders, which can be used
            # by vm tool wrapper to setup the vm volumes, and performs path
            # checking and directory creation).
            newKargs = {}
            for arg, val in itertools.chain(enumerate(args), kargs.items()):
                if arg in argsDecorators:
                    wrapper = argsDecorators[arg]
                    val = wrapper(val)
                newKargs[arg] = val

            # Inject command args within command template, and wrap with vm tool
            # when relevant.
            cmd = None
            if vmType == VMEngine.NONE:
                cmd = str.format(
                    cmdTemplate,
                    executable,
                    **newKargs
                )
            elif vmType == VMEngine.DOCKER:
                cmd = dockerize(
                    cmdTemplate,
                    executable,
                    **newKargs
                )
            elif vmType == VMEngine.SINGULARITY:
                cmd = singularize(
                    cmdTemplate,
                    executable,
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
