import luigi
import sys

class MyTask(luigi.Task):
    def run(self):
        print("Hello from Luigi!")

if __name__ == '__main__':
    luigi.run(sys.argv)