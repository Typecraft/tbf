import io

from tbf.core.constants import HEADER_START, HEADER_END, SEPARATOR, LAYERS_START, LAYERS_END, LAYER_START, \
    LAYER_END, RELATIONS_START, RELATIONS_END, RELATION_START, RELATION_END, ATTRS_START, ATTRS_END, \
    CHUNK_FULL_START, CHUNK_LINKED_START, CHUNK_END
from tbf.core.models import Document, Header, Layer, LayerObject, TBFParsingException


# BACKEND METHODS AND CLASSES #
class _Writer(object):
    def __init__(self, document, output_stream):
        self.document = document
        self.encoding = document.header.encoding
        self.output = output_stream

        self._temp_relations = {}
        self._temp_num_of_relations = 0
        self._temp_attributes = {}
        self._temp_num_of_chunks = 0

    def write(self):
        self._prepare_relations_for_writing()
        self._prepare_attrs_for_writing()

        self._write_header()
        self._write_layers()
        self._write_relations()
        self._write_attrs()

    def _write_header(self):
        self._write_flag(HEADER_START)
        self.output.write(self.document.header.encoding.encode())  # Header has default (utf-8) encoding
        self._write_sep()
        self._write_flag(HEADER_END)

    def _write_layers(self):
        self._write_flag(LAYERS_START)
        self._write_int(len(self.document.layers))
        for layer in self.document.layers:
            self._write_layer(layer)
        self._write_flag(LAYERS_END)

    def _write_layer(self, layer):
        assert isinstance(layer, Layer)
        self._write_flag(LAYER_START)
        self._write_string(layer.name)
        self._write_sep()
        self._write_int(len(layer.objects))
        self._write_flag(LAYER_END)

    def _write_relations(self):
        self._write_flag(RELATIONS_START)
        self._write_int(self._temp_num_of_relations)
        for parent_layer, child_rels in self._temp_relations.items():
            for child_layer, value_tuples in child_rels.items():
                self._write_flag(RELATION_START)
                self._write_int(parent_layer)
                self._write_int(child_layer)
                self._write_int(len(value_tuples))  # Number of pairs

                for tup in value_tuples:
                    self._write_int(tup[0])  # Parent id
                    self._write_int(tup[1])  # Child id
                self._write_flag(RELATION_END)

        self._write_flag(RELATIONS_END)

    def _prepare_relations_for_writing(self):
        for obj in self.document.get_all_objects():
            obj_layer = obj.layer
            if len(obj.children) > 0:
                for child in obj.children:
                    child_layer = child.layer

                    self._temp_relations.setdefault(obj_layer, {})\
                        .setdefault(child_layer, [])\
                        .append((obj.id, child.id))

        # Find total number of relations to write
        for _, parent_layer_rels in self._temp_relations.items():
            self._temp_num_of_relations += len(parent_layer_rels.keys())

    def _write_attrs(self):
        self._write_flag(ATTRS_START)
        self._write_int(self._temp_num_of_chunks)
        for layer_id, layer_attrs in self._temp_attributes.items():
            for attr_name, attr_values in layer_attrs.items():
                # If the overhead of a linked chunk is smaller than for a full chunk
                # we write that.
                layer_objects = self.document.get_layer_by_id(layer_id).objects
                if self._linked_chunk_overhead(len(attr_values.keys())) < self._full_chunk_overhead(len(layer_objects)):
                    self._write_linked_chunk(layer_id, attr_name, attr_values)
                else:
                    self._write_full_chunk(layer_id, attr_name, attr_values)
        self._write_flag(ATTRS_END)

    def _prepare_attrs_for_writing(self):
        for layer in self.document.layers:
            self._temp_attributes[layer.id] = {}
            for obj in layer.objects:
                for key, value in obj.attrs.items():
                    self._temp_attributes[layer.id].setdefault(key, {})[obj.id] = value
            self._temp_num_of_chunks += len(self._temp_attributes[layer.id].keys())

    def _linked_chunk_overhead(self, number_of_entities):
        # 4 bytes per id and 1 per separator
        return number_of_entities * 4 + number_of_entities * 1

    def _full_chunk_overhead(self, number_of_all_entities):
        # 1 per separator
        return number_of_all_entities * 1

    def _write_full_chunk(self, layer_id, attr_name, attr_values):
        self._write_flag(CHUNK_FULL_START)
        self._write_int(layer_id)
        self._write_string(attr_name)
        self._write_sep()

        layer = self.document.get_layer_by_id(layer_id)
        for obj in layer.objects:
            obj_value = attr_values.get(obj.id)
            if obj_value:
                self._write_string(obj_value)
            self._write_sep()
        self._write_flag(CHUNK_END)

    def _write_linked_chunk(self, layer_id, attr_name, attr_values):
        self._write_flag(CHUNK_LINKED_START)
        self._write_int(layer_id)
        self._write_string(attr_name)
        self._write_sep()

        for obj_id, value in attr_values.items():
            self._write_int(obj_id)
            self._write_string(value)
            self._write_sep()

        self._write(CHUNK_END)

    def _write_sep(self):
        self._write_flag(SEPARATOR)

    def _write_flag(self, flag):
        self.output.write(_Writer._ord_to_bytes(flag))

    def _write_int(self, number):
        self.output.write(_Writer._int_to_bytes(number))

    def _write_string(self, _string):
        self.output.write(_string.encode(self.encoding))

    @classmethod
    def _int_to_bytes(cls, number, number_of_bytes=4, endianness='big'):
        return number.to_bytes(number_of_bytes, endianness)

    @classmethod
    def _ord_to_bytes(cls, flag):
        return chr(flag).encode()


class _Parser(object):
    def __init__(self, stream):
        self.stream = io.BufferedReader(stream)  # Grants access to e.g. peek
        self.document = Document()
        self.encoding = 'utf-8'

        self._temp_layers = []
        self._temp_layer_objects = {}

    def parse(self):
        self._parse_header()
        self._parse_layers()
        self._parse_relations()
        self._parse_attrs()

        return self.document

    def _parse_header(self):
        self._expect(HEADER_START)
        encoding = self._eat_until_sep().decode()

        header = Header(encoding=encoding)
        self.encoding = encoding
        self.document.header = header

        self._expect(HEADER_END)

    def _parse_layers(self):
        self._expect(LAYERS_START)
        number_of_layers = self._read_as_int(4)
        for i in range(number_of_layers):
            self._parse_layer(i)

        self._expect(LAYERS_END)

    def _parse_layer(self, _id):
        self._expect(LAYER_START)
        name = self._eat_until_sep().decode(self.encoding)
        number_of_entities = self._read_as_int(4)

        objects = [LayerObject(i, _id) for i in range(number_of_entities)]
        layer = Layer(_id, name, objects)
        self.document.add_layer(layer)
        self._temp_layers.append(layer)
        self._temp_layer_objects[_id] = objects

        self._expect(LAYER_END)

    def _parse_relations(self):
        self._expect(RELATIONS_START)
        number_of_relations = self._read_as_int(4)
        for i in range(number_of_relations):
            self._parse_relation()

        self._expect(RELATIONS_END)

    def _parse_relation(self):
        self._expect(RELATION_START)
        parent_layer = self._read_as_int(4)
        children_layer = self._read_as_int(4)
        num_of_pairs = self._read_as_int(4)

        for i in range(num_of_pairs):
            parent = self._read_as_int(4)
            child = self._read_as_int(4)

            # TODO, add explicit checks and error reporting
            parent_object = self._temp_layer_objects[parent_layer][parent]
            child_object = self._temp_layer_objects[children_layer][child]

            parent_object.add_child(child_object)

        self._expect(RELATION_END)

    def _parse_attrs(self):
        self._expect(ATTRS_START)
        num_of_chunks = self._read_as_int(4)
        for i in range(num_of_chunks):
            self._parse_chunk()

        self._expect(ATTRS_END)

    def _parse_chunk(self):
        _next = self._peek_as_int()
        if _next == CHUNK_FULL_START:
            self._parse_full_chunk()
        elif _next == CHUNK_LINKED_START:
            self._parse_linked_chunk()
        else:
            raise TBFParsingException("Expected CHUNK_FULL_START or CHUNK_LINKED_START, got %d" % _next)

    def _parse_full_chunk(self):
        self._expect(CHUNK_FULL_START)
        layer_id = self._read_as_int(4)
        attr_name = self._eat_until_sep().decode(self.encoding)
        for layer_object in self._temp_layer_objects[layer_id]:
            value = self._eat_until_sep()
            layer_object.set_attr(attr_name, value)

        self._expect(CHUNK_END)

    def _parse_linked_chunk(self):
        self._expect(CHUNK_LINKED_START)
        layer_id = self._read_as_int(4)
        attr_name = self._eat_until_sep().decode(self.encoding)
        num_of_entries = self._read_as_int(4)

        for i in range(num_of_entries):
            object_id = self._read_as_int(4)
            value = self._eat_until_sep()

            layer_object = self._temp_layer_objects[layer_id][object_id]
            layer_object.set_attr(attr_name, value)

        self._expect(CHUNK_END)

    def _read(self, length=1):
        return self.stream.read(length)

    def _read_as_int(self, length=1):
        return int.from_bytes(self.stream.read(length), byteorder='big')

    def _read_as_str(self, length=1):
        return self.stream.read(length).decode(self.encoding)

    def _peek(self, length=1):
        return self.stream.peek(length)[:length]

    def _peek_as_int(self, length=1):
        return int.from_bytes(self.stream.peek(length)[:length], byteorder='big')

    def _expect(self, byte):
        _next = ord(self._read())
        if _next != byte:
            raise TBFParsingException("Expected %d, got %d" % (byte, _next))

    def _eat_until_sep(self):
        temp = b''
        while True:
            _next = self._read()
            if _next == b'' or ord(_next) == SEPARATOR:
                break
            temp += _next
        return temp

