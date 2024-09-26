from mrjob.job import MRJob
from mr3px.csvprotocol import CsvProtocol


class TopK(MRJob):
    INPUT_PROTOCOL = CsvProtocol
    HEADER_SKIPPED = False  # Class-level attribute to track header skipping

    def mapper(self, _, line):
        # Skip the header row
        if not TopK.HEADER_SKIPPED:
            TopK.HEADER_SKIPPED = True
            return

        name = line[1]
        population = int(line[14])
        yield _, (name, population)

    def reducer(self, _, valpair):
        valpair.sort(key= lambda x: x[1])
        yield name, max(population)


if __name__ == '__main__':
    TopK.run()
