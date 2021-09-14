import os


def mayastor_target_dir():
    """Get Mayastor target directory (absolute path) based on evironment variable SRCDIR.
    Raise exception if no Mayastor root is configured.
    """
    if "SRCDIR" not in os.environ:
        raise Exception("SRCDIR environment variable not defined")

    # For now assume only Debug builds, but we might want to consider using
    # a variable to access Release binaries too.
    return "%s/target/debug" % os.environ["SRCDIR"]
