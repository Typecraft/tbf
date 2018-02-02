import io
import pytest
# noinspection PyProtectedMember
from tbf.core.constants import HEADER_START, SEPARATOR, HEADER_END, LAYERS_START, LAYERS_END, LAYER_START, \
    LAYER_END, RELATIONS_START, RELATION_START, RELATION_END, RELATIONS_END, ATTRS_START, CHUNK_FULL_START, \
    CHUNK_END, ATTRS_END, CHUNK_LINKED_START
from tbf.core.models import TBFParsingException, Layer, LayerObject, Document, Header
from tbf.core.parsing import _Parser, _Writer, write_to_string, parse_from_string, write_to_bytes, parse_from_bytes


# noinspection PyProtectedMember
class TestParser(object):
    def test_eat_until_sep(self):
        string = b'\x45\x55\x00'
        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)
        assert parser._eat_until_sep() == b'\x45\x55'

    def test_eat_until_sep_multiple_seps(self):
        string = b'\x45\x00\x55\x00'
        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)
        assert parser._eat_until_sep() == b'\x45'
        assert parser._eat_until_sep() == b'\x55'

    def test_eat_until_sep_multiple_consecutive(self):
        string = b'\x45\x55\x00\x00\x45\x00'
        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)
        assert parser._eat_until_sep() == b'\x45\x55'
        assert parser._eat_until_sep() == b''
        assert parser._eat_until_sep() == b'\x45'

    def test_eat_until_sep_eats_sep_also(self):
        string = b'\x45\x55\x00\x45'
        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)
        assert parser._eat_until_sep() == b'\x45\x55'
        assert parser._peek() == b'\x45'

    def test_eat_until_sep_stops_at_eof(self):
        string = b'\x45\x55'
        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)
        assert parser._eat_until_sep() == b'\x45\x55'

    def test_expect_should_throw_on_bad(self):
        string = b'\x45\x55'
        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)
        with pytest.raises(TBFParsingException):
            parser._expect(0x55)

    def test_expect_should_not_throw_on_good(self):
        string = b'\x45\x55'
        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)
        parser._expect(0x45)

    def read_as_int(self):
        string = (1024).to_bytes(4, byteorder='big')
        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)
        assert parser._read_as_int(4) == 1024

    def test_read_header(self):
        string = chr(HEADER_START).encode()
        string += 'iso-8859-1'.encode()
        string += chr(SEPARATOR).encode()
        string += chr(HEADER_END).encode()

        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)

        parser._parse_header()

        assert parser.encoding == "iso-8859-1"
        assert parser.document.header is not None
        assert parser.document.header.encoding == "iso-8859-1"

    def test_read_a_few_layers(self):
        string = chr(LAYERS_START).encode()
        string += (1).to_bytes(4, 'big')
        string += chr(LAYER_START).encode()
        string += b'LayerName'
        string += chr(SEPARATOR).encode()
        string += (50).to_bytes(4, 'big')
        string += chr(LAYER_END).encode()
        string += chr(LAYERS_END).encode()

        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)

        parser._parse_layers()
        assert len(parser._temp_layers) == 1
        assert len(parser._temp_layer_objects[0]) == 50
        layer = parser._temp_layers[0]
        assert isinstance(layer, Layer)
        assert layer.name == 'LayerName'
        assert len(layer.objects) == 50

        for i in range(len(layer.objects)):
            assert layer.objects[i].layer == 0
            assert layer.objects[i].id == i

    def test_read_relations(self):
        string = chr(RELATIONS_START).encode()
        string += (1).to_bytes(4, 'big')  # Number of relations
        string += chr(RELATION_START).encode()
        string += (0).to_bytes(4, 'big')  # Layer 0
        string += (1).to_bytes(4, 'big')  # Layer 1
        string += (1).to_bytes(4, 'big')  # Number of relational pairs for Layer 0 to Layer 1
        string += (0).to_bytes(4, 'big')  # Entity in Layer 0
        string += (0).to_bytes(4, 'big')  # Entity in Layer 1
        string += chr(RELATION_END).encode()
        string += chr(RELATIONS_END).encode()

        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)

        # Mock layers and objects
        parser._temp_layers = [
            Layer(0, 'name'),
            Layer(1, 'name')
        ]

        layer_object_1 = LayerObject(0, 0)
        layer_object_2 = LayerObject(0, 1)

        parser._temp_layer_objects = {
            0: [layer_object_1],
            1: [layer_object_2],
        }

        parser._parse_relations()

        assert len(layer_object_1.children) == 1
        assert layer_object_1.children[0] == layer_object_2

    def test_read_attrs_with_full_chunk(self):
        string = chr(ATTRS_START).encode()
        string += (1).to_bytes(4, 'big')  # Number of chunks
        string += chr(CHUNK_FULL_START).encode()
        string += (0).to_bytes(4, 'big')  # Layer id
        string += b'attribute'  # Attribute name
        string += chr(SEPARATOR).encode()
        string += b'value1'
        string += chr(SEPARATOR).encode()
        string += b'value2'
        string += chr(SEPARATOR).encode()
        string += chr(CHUNK_END).encode()
        string += chr(ATTRS_END).encode()

        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)
        # Mock layers and objects
        parser._temp_layers = [
            Layer(0, 'name'),
        ]

        layer_object_1 = LayerObject(0, 0)
        layer_object_2 = LayerObject(1, 0)
        parser._temp_layer_objects = {
            0: [layer_object_1, layer_object_2]
        }

        parser._parse_attrs()

        assert 'attribute' in layer_object_1.attrs
        assert 'attribute' in layer_object_2.attrs

        assert layer_object_1.attrs['attribute'] == b'value1'
        assert layer_object_2.attrs['attribute'] == b'value2'

    def test_read_attrs_with_linked_chunk(self):
        string = chr(ATTRS_START).encode()
        string += (1).to_bytes(4, 'big')  # Number of chunks
        string += chr(CHUNK_LINKED_START).encode()
        string += (0).to_bytes(4, 'big')  # Layer id
        string += b'attribute'  # Attribute name
        string += chr(SEPARATOR).encode()
        string += (2).to_bytes(4, 'big')  # Number of entities
        string += (0).to_bytes(4, 'big')  # Entity id
        string += b'value1'  # Value 1
        string += chr(SEPARATOR).encode()
        string += (1).to_bytes(4, 'big')  # Entity id
        string += b'value2'  # Value 1
        string += chr(SEPARATOR).encode()
        string += chr(CHUNK_END).encode()
        string += chr(ATTRS_END).encode()

        stream_form = io.BytesIO(string)
        parser = _Parser(stream_form)
        # Mock layers and objects
        parser._temp_layers = [
            Layer(0, 'name'),
        ]

        layer_object_1 = LayerObject(0, 0)
        layer_object_2 = LayerObject(1, 0)
        parser._temp_layer_objects = {
            0: [layer_object_1, layer_object_2]
        }

        parser._parse_attrs()

        assert 'attribute' in layer_object_1.attrs
        assert 'attribute' in layer_object_2.attrs

        assert layer_object_1.attrs['attribute'] == b'value1'
        assert layer_object_2.attrs['attribute'] == b'value2'


class TestWriter(object):
    def test_write_flag(self):
        output = io.BytesIO()
        document = Document(header=Header(encoding="utf-8"))

        writer = _Writer(document, output)

        writer._write_flag(HEADER_START)
        assert output.getvalue() == b'\x01'

    def test_write_sep(self):
        output = io.BytesIO()
        document = Document(header=Header(encoding="utf-8"))

        writer = _Writer(document, output)

        writer._write_sep()
        assert output.getvalue() == b'\x00'

    def test_write_int(self):
        output = io.BytesIO()
        document = Document(header=Header(encoding="utf-8"))

        writer = _Writer(document, output)

        # We assert 4 bytes per int here
        writer._write_int(128)
        assert output.getvalue() == b'\x00\x00\x00\x80'

    def test_write_string(self):
        output = io.BytesIO()
        document = Document(header=Header(encoding="utf-8"))

        writer = _Writer(document, output)

        # We assert 4 bytes per int here
        writer._write_string('Hello')
        assert output.getvalue() == b'Hello'

    def test_prepare_relations_for_writing(self):
        output = io.BytesIO()
        document = Document(header=Header(encoding="utf-8"))

        layer_1 = Layer(0, "Layer 1")
        layer_2 = Layer(1, "Layer 2")

        document.add_layer(layer_1)
        document.add_layer(layer_2)

        objs_layer_1 = [
            LayerObject(0, 0),
            LayerObject(1, 0),
            LayerObject(2, 0),
            LayerObject(3, 0),
        ]

        objs_layer_2 = [
            LayerObject(0, 1),
            LayerObject(1, 1),
            LayerObject(2, 1),
            LayerObject(3, 1),
        ]

        layer_1.add_objects(objs_layer_1)
        layer_2.add_objects(objs_layer_2)

        # Add some relations
        objs_layer_1[0].add_child(objs_layer_2[1])
        objs_layer_1[1].add_child(objs_layer_2[0])
        objs_layer_1[2].add_child(objs_layer_2[3])
        objs_layer_1[3].add_child(objs_layer_2[2])

        writer = _Writer(document, output)
        writer._prepare_relations_for_writing()

        assert 0 in writer._temp_relations
        assert 1 in writer._temp_relations[0]

        rel_tuples = writer._temp_relations[0][1]
        assert (0, 1) in rel_tuples
        assert (1, 0) in rel_tuples
        assert (2, 3) in rel_tuples
        assert (3, 2) in rel_tuples

        assert writer._temp_num_of_relations == 1

    def test_prepare_attrs_for_writing(self):
        output = io.BytesIO()
        document = Document(header=Header(encoding="utf-8"))

        layer_1 = Layer(0, "Layer 1")
        layer_2 = Layer(1, "Layer 2")

        document.add_layer(layer_1)
        document.add_layer(layer_2)

        objs_layer_1 = [
            LayerObject(0, 0, attrs={'key1': 'val2'}),
            LayerObject(1, 0, attrs={'key2': 'val'}),
            LayerObject(2, 0, attrs={'key1': 'val4', 'key2': 'val3'}),
            LayerObject(3, 0, attrs={'key2': 'val'}),
        ]

        objs_layer_2 = [
            LayerObject(0, 1, attrs={'key3': 'val'}),
            LayerObject(1, 1, attrs={'key4': 'val6'}),
            LayerObject(2, 1, attrs={'key2': 'val7'}),
            LayerObject(3, 1, attrs={'key4': 'val2'}),
        ]

        layer_1.add_objects(objs_layer_1)
        layer_2.add_objects(objs_layer_2)

        # Add some relations
        objs_layer_1[0].add_child(objs_layer_2[1])
        objs_layer_1[1].add_child(objs_layer_2[0])
        objs_layer_1[2].add_child(objs_layer_2[3])
        objs_layer_1[3].add_child(objs_layer_2[2])

        writer = _Writer(document, output)
        writer._prepare_attrs_for_writing()

        assert 0 in writer._temp_attributes
        assert 1 in writer._temp_attributes

        layer_0_attrs = writer._temp_attributes[0]
        assert 'key1' in layer_0_attrs
        assert 'key2' in layer_0_attrs

        assert 0 in layer_0_attrs['key1']
        assert layer_0_attrs['key1'][0] == 'val2'
        assert 2 in layer_0_attrs['key1']
        assert layer_0_attrs['key1'][2] == 'val4'

        assert 1 in layer_0_attrs['key2']
        assert layer_0_attrs['key2'][1] == 'val'
        assert 2 in layer_0_attrs['key2']
        assert layer_0_attrs['key2'][2] == 'val3'
        assert 3 in layer_0_attrs['key2']
        assert layer_0_attrs['key2'][3] == 'val'

        layer_1_attrs = writer._temp_attributes[1]
        assert 'key2' in layer_1_attrs
        assert 'key3' in layer_1_attrs
        assert 'key4' in layer_1_attrs

        assert 2 in layer_1_attrs['key2']
        assert layer_1_attrs['key2'][2] == 'val7'

        assert 0 in layer_1_attrs['key3']
        assert layer_1_attrs['key3'][0] == 'val'

        assert 1 in layer_1_attrs['key4']
        assert layer_1_attrs['key4'][1] == 'val6'
        assert 3 in layer_1_attrs['key4']
        assert layer_1_attrs['key4'][3] == 'val2'

    def test_write_header(self):
        output = io.BytesIO()
        document = Document(header=Header(encoding="utf-8"))

        writer = _Writer(document, output)
        writer._write_header()

        assert output.getvalue() == b'\x01utf-8\x00\x02'

    def test_write_layers(self):
        output = io.BytesIO()
        document = Document(header=Header(encoding="utf-8"))

        layer_1 = Layer(0, "Layer 1")
        layer_2 = Layer(1, "Layer 2")

        document.add_layer(layer_1)
        document.add_layer(layer_2)

        objs_layer_1 = [
            LayerObject(0, 0),
            LayerObject(1, 0),
            LayerObject(2, 0),
            LayerObject(3, 0),
        ]

        objs_layer_2 = [
            LayerObject(0, 1),
            LayerObject(1, 1),
            LayerObject(2, 1),
            LayerObject(3, 1),
        ]

        layer_1.add_objects(objs_layer_1)
        layer_2.add_objects(objs_layer_2)

        writer = _Writer(document, output)
        writer._write_layers()

        assert output.getvalue() == b'\x03' \
                                    b'\x00\x00\x00\x02' \
                                    b'\x09Layer 1\x00\x00\x00\x00\x04\x0A' \
                                    b'\x09Layer 2\x00\x00\x00\x00\x04\x0A' \
                                    b'\x04'

    def test_write_relations(self):
        output = io.BytesIO()
        document = Document(header=Header(encoding="utf-8"))

        layer_1 = Layer(0, "Layer 1")
        layer_2 = Layer(1, "Layer 2")

        document.add_layer(layer_1)
        document.add_layer(layer_2)

        objs_layer_1 = [
            LayerObject(0, 0),
            LayerObject(1, 0),
            LayerObject(2, 0),
            LayerObject(3, 0),
        ]

        objs_layer_2 = [
            LayerObject(0, 1),
            LayerObject(1, 1),
            LayerObject(2, 1),
            LayerObject(3, 1),
        ]

        layer_1.add_objects(objs_layer_1)
        layer_2.add_objects(objs_layer_2)

        # Add some relations
        objs_layer_1[0].add_child(objs_layer_2[1])
        objs_layer_1[1].add_child(objs_layer_2[0])
        objs_layer_1[2].add_child(objs_layer_2[3])
        objs_layer_1[3].add_child(objs_layer_2[2])

        writer = _Writer(document, output)
        writer._prepare_relations_for_writing()
        writer._write_relations()

        assert output.getvalue() == b'\x07' \
                                    b'\x00\x00\x00\x01' \
                                    b'\x0e' \
                                    b'\x00\x00\x00\x00' \
                                    b'\x00\x00\x00\x01' \
                                    b'\x00\x00\x00\x04' \
                                    b'\x00\x00\x00\x00' \
                                    b'\x00\x00\x00\x01' \
                                    b'\x00\x00\x00\x01' \
                                    b'\x00\x00\x00\x00' \
                                    b'\x00\x00\x00\x02' \
                                    b'\x00\x00\x00\x03' \
                                    b'\x00\x00\x00\x03' \
                                    b'\x00\x00\x00\x02' \
                                    b'\x0f' \
                                    b'\x08'

    def test_write_attrs(self):
        output = io.BytesIO()
        document = Document(header=Header(encoding="utf-8"))

        layer_1 = Layer(0, "Layer 1")
        layer_2 = Layer(1, "Layer 2")

        document.add_layer(layer_1)
        document.add_layer(layer_2)

        objs_layer_1 = [
            LayerObject(0, 0, attrs={'key1': 'val2'}),
        ]

        objs_layer_2 = [
            LayerObject(0, 1, attrs={'key3': 'val'}),
            LayerObject(1, 1, attrs={'key4': 'val2'}),
        ]

        layer_1.add_objects(objs_layer_1)
        layer_2.add_objects(objs_layer_2)

        writer = _Writer(document, output)
        writer._prepare_attrs_for_writing()
        writer._write_attrs()

        # All chunks will be full
        # The ordering may vary, so we simply that the correct chunks are there.
        output_value = output.getvalue()
        assert b'\x05' \
               b'\x00\x00\x00\x03' in output_value
        assert b'\x0B' \
               b'\x00\x00\x00\x00' \
               b'key1\x00' \
               b'val2\x00' \
               b'\x0D' in output_value
        assert b'\x0B' \
               b'\x00\x00\x00\x01' \
               b'key3\x00' \
               b'val\x00\x00' \
               b'\x0D' in output_value
        assert b'\x0B' \
               b'\x00\x00\x00\x01' \
               b'key4\x00' \
               b'\x00val2\x00' \
               b'\x0D'in output_value
        assert b'\x06' in output_value[-1:]


class TestFrontendMethods(object):
    def test_write_to_string(self):
        document = Document(header=Header(encoding="utf-8"))

        layer_1 = Layer(0, "Layer 1")
        layer_2 = Layer(1, "Layer 2")

        document.add_layer(layer_1)
        document.add_layer(layer_2)

        objs_layer_1 = [
            LayerObject(0, 0, attrs={'key1': 'val2'}),
        ]

        objs_layer_2 = [
            LayerObject(0, 1, attrs={'key3': 'val'}),
            LayerObject(1, 1, attrs={'key4': 'val2'}),
        ]

        layer_1.add_objects(objs_layer_1)
        layer_2.add_objects(objs_layer_2)

        output = write_to_string(document)
        assert isinstance(output, str)


class TestIntegrationParseWrite(object):
    def test_write_then_parse_goes_through(self):
        document = Document(header=Header(encoding="utf-8"))

        layer_1 = Layer(0, "Layer 1")
        layer_2 = Layer(1, "Layer 2")

        document.add_layer(layer_1)
        document.add_layer(layer_2)

        objs_layer_1 = [
            LayerObject(0, 0, attrs={'key1': 'val2'}),
        ]

        objs_layer_2 = [
            LayerObject(0, 1, attrs={'key3': 'val'}),
            LayerObject(1, 1, attrs={'key4': 'val2'}),
        ]

        layer_1.add_objects(objs_layer_1)
        layer_2.add_objects(objs_layer_2)

        output = write_to_bytes(document)
        parsed = parse_from_bytes(output)

        assert isinstance(parsed, Document)
        assert len(parsed.layers) == 2
        assert parsed.layers[0].name == 'Layer 1'
        assert parsed.layers[1].name == 'Layer 2'

        layer_1 = parsed.layers[0]
        layer_2 = parsed.layers[1]

        assert len(layer_1.objects) == 1
        assert layer_1.objects[0].id == 0
        assert layer_1.objects[0].layer == 0
        assert 'key1' in layer_1.objects[0].attrs
        assert layer_1.objects[0].attrs['key1'] == b'val2'

        assert len(layer_2.objects) == 2
        assert layer_2.objects[0].id == 0
        assert layer_2.objects[0].layer == 1
        assert 'key3' in layer_2.objects[0].attrs
        assert layer_2.objects[0].attrs['key3'] == b'val'
        assert 'key4' in layer_2.objects[1].attrs
        assert layer_2.objects[1].attrs['key4'] == b'val2'

        output = write_to_string(document)
        parsed = parse_from_string(output)

        assert isinstance(parsed, Document)
        assert len(parsed.layers) == 2
        assert parsed.layers[0].name == 'Layer 1'
        assert parsed.layers[1].name == 'Layer 2'

        layer_1 = parsed.layers[0]
        layer_2 = parsed.layers[1]

        assert len(layer_1.objects) == 1
        assert layer_1.objects[0].id == 0
        assert layer_1.objects[0].layer == 0
        assert 'key1' in layer_1.objects[0].attrs
        assert layer_1.objects[0].attrs['key1'] == b'val2'

        assert len(layer_2.objects) == 2
        assert layer_2.objects[0].id == 0
        assert layer_2.objects[0].layer == 1
        assert 'key3' in layer_2.objects[0].attrs
        assert layer_2.objects[0].attrs['key3'] == b'val'
        assert 'key4' in layer_2.objects[1].attrs
        assert layer_2.objects[1].attrs['key4'] == b'val2'


