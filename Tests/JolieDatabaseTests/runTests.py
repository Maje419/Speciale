import subprocess

for i in range(0, 100):
    print("Running test number {}\n".format(i))
    subprocess.run(["npm",  "run", "timeHSQL"])