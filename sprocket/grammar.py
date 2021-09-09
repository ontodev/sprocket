from lark import Lark, Transformer


class SprocketTransformer(Transformer):
    def NOT(self, args):
        return "not"

    def OPERATOR(self, args):
        return str(args)

    def WORD(self, args):
        return args

    def value(self, args):
        v = args[0]
        if v.type == "NUMBER":
            return int(v)
        elif v.type == "ESCAPED_STRING":
            # Remove surrounding quotes and unescape any internal quotes
            return str(v)[1:-1].replace('\\"', '"')
        else:
            return str(v)

    def lst(self, args):
        return args

    def constraint(self, args):
        return args[0]

    def start(self, args):
        return args


PARSER = Lark(
    """
NOT: "not."
OPERATOR: "eq" | "gt" | "gte" | "lt" | "lte" | "neq" | "like" | "ilike" | "in" | "is"
// | "fts" | "plfts" | "phfts" | "wfts" | "cs" | "cd" | "ov" | "sl" | "sr" | "nxr" | "nxl" | "adj"

WORD: /[^,"()]+/

value: NUMBER | ESCAPED_STRING | WORD

lst : "(" value ("," value)* ")"

constraint: lst | value

start: NOT OPERATOR "." constraint | OPERATOR "." constraint

%import common.ESCAPED_STRING
%import common.NUMBER
"""
)
