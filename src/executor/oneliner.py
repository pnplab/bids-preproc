# Strip command from newlines / tabs.
def oneliner(cmd):
    return cmd.replace('\n', ' ').replace('    ', '')
