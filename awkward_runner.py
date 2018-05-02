import luigi
from faker import Faker
from datetime import datetime

class AwkwardFileWriter(luigi.Task):
    # Has no requirements

    date = luigi.DateParameter()

    def run(self):

        fake = Faker()

        with self.output().open("w") as output:
            for i in range(1000):
                output.write('{}\t{}\t{}\n'.format(
                    fake.sentence(),
                    fake.sentence(),
                    fake.sentence()
                ))

    def output(self):
        return luigi.LocalTarget(self.date.strftime('./files/words_%Y_%m_%d_%H_%M_%S_faked.tsv'))


class AwkwardFileReader(luigi.Task):

    def requires(self):
        return [AwkwardFileWriter(date=datetime.now())]

    def run(self):
        with self.output().open("w") as output:
            for f in self.input():
                print(f)
                for line in f.open('r'):
                    output.write('{}\n'.format(" ".join(line.split("\t"))))

    def output(self):
        return luigi.LocalTarget('./files/sentences_final_faked.tsv')


if __name__ == "__main__":
    luigi.build([AwkwardFileReader()])

