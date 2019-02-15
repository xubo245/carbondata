class CarbonWriter(object):
    def __init__(self, java_entry):
        self.java_entry = java_entry

    def builder(self):
        self.CarbonWriterBuilder = self.java_entry.createCarbonWriterBuilder()
        return self

    def outputPath(self, path):
        self.CarbonWriterBuilder.outputPath(path)
        return self

    def withCsvInput(self, jsonSchema):
        self.CarbonWriterBuilder.withCsvInput(jsonSchema)
        return self

    def writtenBy(self, name):
        self.CarbonWriterBuilder.writtenBy(name)
        return self

    def withHadoopConf(self, key, value):
        self.CarbonWriterBuilder.withHadoopConf(key, value)
        return self

    def build(self):
        self.writer = self.CarbonWriterBuilder.build()
        return self

    def write(self, object):
        return self.writer.write(object)

    def close(self):
        return self.writer.close()
