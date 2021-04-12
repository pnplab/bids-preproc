class TaskConfig():
    def __init__(self, raw_executable: str, singularity_image: str,
                 docker_image: str, cmd: str, decorators):
        self.raw_executable = raw_executable
        self.singularity_image = singularity_image
        self.docker_image = docker_image
        self.cmd = cmd
        self.decorators = decorators
