
import subprocess


class MiniDfs:

    def __init__(self):
        self.child = subprocess.Popen(
            [
                "mvn",
                "-f",
                "../rust/minidfs",
                "--quiet",
                "clean",
                "compile",
                "exec:java",
            ],
            stdout=subprocess.PIPE,
            # stderr=subprocess.DEVNULL,
            universal_newlines=True,
            encoding="utf8",
            bufsize=0
        )

        output = self.child.stdout.readline().strip()
        assert output == "Ready!", output

    def get_url(self):
        return "hdfs://127.0.0.1:9000"
    
    def __del__(self):
        self.child.kill()