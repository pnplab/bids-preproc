# @warning
# set -o pipefail is only compatible with bash, not dash (debian).
def logify(cmd, logPath):
    # @warning
    # relying on `tee` can be/is memory and bandwidth hungry regarding fmriprep
    # output for instance, as log is transfered (if dask) and stored within
    # the result object.
    # return f'''(set -o pipefail && {cmd} 2>&1 | tee "{logPath}")'''
    return f'''(set -o pipefail && {cmd} > "{logPath}" 2>&1)'''
