
import subprocess
import pytest

@pytest.fixture
def minidfs():
    child = subprocess.Popen(
        [
            "mvn",
            "-f",
            "../crates/hdfs-native/minidfs",
            "--quiet",
            "clean",
            "compile",
            "exec:java",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        universal_newlines=True,
        encoding="utf8",
        bufsize=0
    )

    output = child.stdout.readline().strip()
    assert output == "Ready!", output

    yield "hdfs://127.0.0.1:9000"

    try:
        child.communicate(input="\n", timeout=30)
    except:
        child.kill()
