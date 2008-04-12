__all__ = ['create_enum_type', 'create_bitfield_type']
from sqlalchemy import SmallInteger
from sqlalchemy.types import TypeDecorator

#
# Some commonly useful types
#

def create_enum_type(name, enum):
    class MappingType(TypeDecorator):
        impl = SmallInteger
        def process_bind_param(self, value, engine):
            if value is None:
                return None
            return int(repr(value)) # ugh, work around the fact that int(int-subclass) doesn't turn it into an int
        # the (probably more correct) alternative would be to register a psycopg2 adapter for this class
        def process_result_value(self, value, engine):
            if value is None:
                return None
            return enum.values[value]
    MappingType.__name__ = name
    return MappingType

def create_bitfield_type(name, bitfield):
    # TODO: for MySQL, translate into a set field
    class BitfieldType(TypeDecorator):
        impl = SmallInteger
        def process_bind_param(self, value, engine):
            if value is None:
                return 0
            x = 0
            assert type(value) is set
            for v in value:
                assert type(v) is bitfield._type_
                x |= v
            return x
        def process_result_value(self, value, engine):
            # not very fast
            result = set()
            for v in bitfield.values:
                if v & value:
                    result.add(v)
            return result
    BitfieldType.__name__ = name
    return BitfieldType
