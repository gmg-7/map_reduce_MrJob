"""
a = "geonameid,name,asciiname,alternatenames,latitude,longitude,feature class,feature code,country code,cc2,admin1 code,admin2 code,admin3 code,admin4 code,population,elevation,dem,timezone,modification date"

i = 0
for word in a.split(','):
    print(word, i)
    i += 1

"""
from mrjob.job import MRJob
from mr3px.csvprotocol import CsvProtocol


class ProjectionDuplicateRemoval(MRJob):
    INPUT_PROTOCOL = CsvProtocol
    HEADER_SKIPPED = False  # Class-level attribute to track header skipping

    def mapper(self, _, line):
        # Skip the header row
        if not ProjectionDuplicateRemoval.HEADER_SKIPPED:
            ProjectionDuplicateRemoval.HEADER_SKIPPED = True
            return

        name = line[1]
        population = int(line[14])
        yield name, population

    def reducer(self, name, population):
        yield name, max(population)


if __name__ == '__main__':
    ProjectionDuplicateRemoval.run()
