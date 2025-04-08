import luigi

class HelloWorldTask(luigi.Task):
    def run(self):
        print("Executing HelloWorldTask")
        with self.output().open('w') as f:
            f.write("Hello, Luigi!")

    def output(self):
        return luigi.LocalTarget("hello_world.txt")

if __name__ == '__main__':
    luigi.run()
