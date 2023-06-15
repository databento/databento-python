"""
Functions for handling challenge-response authentication.
"""
import argparse
import hashlib
import os
import sys


BUCKET_ID_LENGTH = 5


def get_challenge_response(challenge: str, key: str) -> str:
    """
    Return the response for a given challenge-response authentication mechanism
    (CRAM) code provided by a Databento service.

    A valid API key is hashed with the challenge string.

    Parameters
    ----------
    challenge : str
        The CRAM challenge string.
    key : str
        The user API key for authentication.

    Returns
    -------
    str

    """
    bucket_id = key[-BUCKET_ID_LENGTH:]
    sha = hashlib.sha256(f"{challenge}|{key}".encode()).hexdigest()
    return f"{sha}-{bucket_id}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script for computing a CRAM response.",
    )
    parser.add_argument(
        "challenge",
        help="The CRAM challenge string",
    )
    parser.add_argument(
        "key",
        nargs="?",
        default=os.environ.get("DATABENTO_API_KEY"),
        help="An API key; defaults to the value of DATABENTO_API_KEY if set",
    )
    arguments = parser.parse_args(sys.argv[1:])

    if arguments.key is None:
        parser.print_usage()
        exit(1)

    print(
        get_challenge_response(
            challenge=arguments.challenge,
            key=arguments.key,
        ),
    )
