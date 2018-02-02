class TBFParsingException(Exception):
    pass


class Document(object):
    def __init__(self, layers=None, header=None):
        self.layers = layers or []
        self.layers_by_id = {layer.id: layer for layer in self.layers}
        self.header = header

    def add_layer(self, layer):
        assert isinstance(layer, Layer)
        self.layers.append(layer)
        self.layers_by_id[layer.id] = layer

    def add_layers(self, layers):
        for layer in layers:
            self.add_layer(layer)

    def get_all_objects(self):
        return [obj for layer in self.layers for obj in layer.objects]

    def get_layer_by_id(self, layer_id):
        return self.layers_by_id[layer_id]


class Header(object):
    def __init__(self, encoding="utf-8"):
        self.encoding = encoding


class Layer(object):
    def __init__(self, _id, name, objects=None):
        self.id = _id
        self.name = name
        self.objects = objects or []

    def add_object(self, obj):
        assert isinstance(obj, LayerObject)
        obj.layer = self.id

        self.objects.append(obj)

    def add_objects(self, objects):
        for obj in objects:
            self.add_object(obj)


class LayerObject(object):
    def __init__(self, _id, layer_id, children=None, attrs=None):
        self.id = _id
        self.layer = layer_id
        self.children = children or []
        self.attrs = attrs or {}

    def add_child(self, child):
        self.children.append(child)

    def set_attr(self, key, value):
        self.attrs[key] = value
