import argparse


def readCLIArgs():
    parser = argparse.ArgumentParser(
        description='Preprocess fmri bids dataset.'
    )
    parser.add_argument(
        'datasetPath',
        type=str,
        help='path of the input bids dataset'
    )

    args = parser.parse_args()
    return args
