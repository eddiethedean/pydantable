from . import readers
from . import transformers
from . import validators
from . import writers


def csv_reader(f) -> readers.CSVDictReaderPipe:
    return readers.CSVDictReaderPipe(
        f,
        transformers.DictTransformerPipe,
        validators.DictValidatorPipe,
        writers.CSVDictWriterPipe
    )

def validator(data, model) -> validators.DictValidatorPipe:
    return validators.DictValidatorPipe(
        data,
        model,
        transformers.DictTransformerPipe,
        validators.DictValidatorPipe,
        writers.CSVDictWriterPipe
    )

def transformer(data, transfomer_function) -> transformers.DictTransformerPipe:
    return transformers.DictTransformerPipe(
        data,
        transfomer_function,
        transformers.DictTransformerPipe,
        validators.DictValidatorPipe,
        writers.CSVDictWriterPipe
    )

def csv_writer(data, f) -> writers.CSVDictWriterPipe:
    return writers.CSVDictWriterPipe(
        data,
        f,
        transformers.DictTransformerPipe,
        validators.DictValidatorPipe,
        writers.CSVDictWriterPipe
    )