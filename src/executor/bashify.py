# @warning output cannot be merged into a single.
def bashify(cmd):
    # We need heredoc syntax in order to allow inner cmd quotes without
    # extra escaping. Quotes around __BASH_CMD_END__ allow to selectively
    # escape from bash dollar interpolation or not.
    # cf. https://stackoverflow.com/a/27921346/939741
    #
    # example input with mixed python / bash interpolation / escaping:
    # {0} -l "{archiveDir}/{archiveName}" --list-format=normal
    #     | awk "NR > 2 {{ print \$NF; }}"
    #     | grep 'sub-'
    #     | sed -E "s;^sub-([^/]+)/?(ses-([^/]+))?.*;\\1,\\3;g"
    #     | sed -E "s/,?\$//g"
    #     | sort | uniq
    return ('bash <<\'__BASH_CMD_END__\'' '\n'
            f'{cmd}' '\n'
            '__BASH_CMD_END__')
